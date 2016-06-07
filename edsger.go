/*
Package edsger provides a portable, high-performance, interprocess semaphore
library.

Named after the father of the semaphore construct, Dr. Edsger Dijkstra, this
package promises a Go-native, user-space semaphore-like locking service via a
light-weight, fast, performant TCP service. Every call to Lock, Unlock, RLock,
and RUnlock will start the service on its assigned port (default 36883) if the
server is not yet running. Additionally, all communications between the client
and server are extremely small with request payloads only 17 bytes in size and
replies a single byte.

Creating and using a named Semaphore is simple:

        var sema = Semaphore{Name: "MyNamedSemaphore"}
        sema.Lock()
        sema.Unlock()

The above example creates a new, named Semaphore and then uses it to obtain
and release a write lock. For more information on unnamed and named Semaphores,
please see the Semaphore type's description.

The server is started when the package is loaded, so the implemented method for
runtime configuration is environment variables:

        EDSGER_LOCK_PORT     the port on which lock requests are received

        EDSGER_UNLOCK_PORT   the port on which unlock requests are received

        EDSGER_PID_FILE      the path to the pid file used to track whether or
                             not the server is running

        EDSGER_MAX_CONN      the maximum number of allowed, concurrent
                             connections. please note that since the lock and
                             unlock requests are handled via separate endpoints,
                             the unlock requests will never be blocked if there
                             are no available connections due to a high number
                             of lock requests blocking. the value -1 indicates
                             that the system's number of logical CPU cores will
                             be used

        EDSGER_WAIT          the duration to wait in-between loop iterations
                             inside the watchdog goroutine that ensures the
                             server is running

        EDSGER_LOG_LEVEL     the log level for the package. valid values are
                             panic, error, warn, info, and debug. the default
                             value is panic
*/
package edsger

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// lockPort is the TCP port on which the server listens for lock requests.
	// The default port is 36883.
	lockPort = 36883

	// unlockPort is the TCP port on which the server listens for unlock
	// requests. The default port is 36883.
	unlockPort = 36884

	// PIDFile is the path to the server's PID file.
	pidFilePath = "/var/run/edsger.pid"

	// serverWait is the duration that the server watchdogs loops sleep.
	serverWait = time.Duration(1 * time.Second)

	// maxConnections is the maximum number of clients the server will accept
	// at once before queuing. specify 0 to remove any throttling. specify -1
	// to set the limit to the number of the system's logical CPU cores. the
	// default value is -1.
	maxConnections = -1

	// lockAddr is the server's TCP endpoint for lock requests
	lockAddr *net.TCPAddr

	// ulckAddr is the server's TCP endpoint for unlock requests
	ulckAddr *net.TCPAddr

	// errNameLen occurs when a Semaphore's name is longer than 256 characters.
	errNameLen = fmt.Errorf("name too long")

	// lcks tracks the server-side mutexes that map to the clients' Semaphores
	lcks = map[string]*sync.RWMutex{}

	// wmen is used to synchronize access to the lcks map
	wmen = sync.RWMutex{}

	// log is the package's logger
	log *logger
)

const (
	// proto is the network protocol used by Edsger
	proto = "ip"
)

type logLevel int

const (
	panicLevel logLevel = iota
	errorLevel
	warnLevel
	infoLevel
	debugLevel
)

func parseLogLevel(s string) logLevel {
	switch strings.ToLower(s) {
	case "error":
		return errorLevel
	case "warn":
		return warnLevel
	case "info":
		return infoLevel
	case "debug":
		return debugLevel
	default:
		return panicLevel
	}
}

func (l logLevel) String() string {
	switch l {
	case panicLevel:
		return "PANIC"
	case errorLevel:
		return "ERROR"
	case warnLevel:
		return "WARN"
	case infoLevel:
		return "INFO"
	case debugLevel:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

type logger struct {
	logLvl logLevel
}

func (l *logger) log(msgLvl logLevel, args ...interface{}) {

	if msgLvl > l.logLvl || len(args) == 0 {
		return
	}

	var (
		b   *bytes.Buffer
		msg string
		w   io.Writer = os.Stderr
		now           = time.Now().Format("15:04:05.000000000")
	)

	if msgLvl == panicLevel {
		b = &bytes.Buffer{}
		w = io.MultiWriter(b, os.Stderr)
	}

	switch t0 := args[0].(type) {
	case error:
		msg = t0.Error()
	case fmt.Stringer:
		msg = t0.String()
	case string:
		if len(args) > 1 {
			msg = fmt.Sprintf(t0, args[1:]...)
		} else {
			msg = t0
		}
	}

	fmt.Fprintf(w, "[%s] %s %s\n", msgLvl, now, msg)

	if msgLvl == panicLevel {
		panic(b.String())
	}
}

func (l *logger) panicf(args ...interface{}) {
	l.log(panicLevel, args...)
}
func (l *logger) errorf(args ...interface{}) {
	l.log(errorLevel, args...)
}
func (l *logger) warnf(args ...interface{}) {
	l.log(warnLevel, args...)
}
func (l *logger) infof(args ...interface{}) {
	l.log(infoLevel, args...)
}
func (l *logger) debugf(args ...interface{}) {
	l.log(debugLevel, args...)
}

func init() {

	var (
		i   int
		v   string
		err error
		d   time.Duration
	)

	if v = os.Getenv("EDSGER_LOCK_PORT"); v != "" {
		if i, err = strconv.Atoi(v); err != nil {
			lockPort = i
		}
	}

	if v = os.Getenv("EDSGER_UNLOCK_PORT"); v != "" {
		if i, err = strconv.Atoi(v); err != nil {
			unlockPort = i
		}
	}

	if v = os.Getenv("EDSGER_PID_FILE"); v != "" {
		pidFilePath = v
	}

	if v = os.Getenv("EDSGER_MAX_CONN"); v != "" {
		if i, err = strconv.Atoi(v); err != nil {
			maxConnections = i
		}
	}
	if maxConnections == -1 {
		maxConnections = runtime.NumCPU()
	}

	if v = os.Getenv("EDSGER_WAIT"); v != "" {
		if d, err = time.ParseDuration(v); err != nil {
			serverWait = d
		}
	}

	// create the package's logger
	log = &logger{parseLogLevel(os.Getenv("EDSGER_LOG_LEVEL"))}

	// print the defaults
	log.infof("EDSGER_LOCK_PORT=%d", lockPort)
	log.infof("EDSGER_UNLOCK_PORT=%d", unlockPort)
	log.infof("EDSGER_PID_FILE=%s", pidFilePath)
	log.infof("EDSGER_MAX_CONN=%d", maxConnections)
	log.infof("EDSGER_WAIT=%s", serverWait.String())
	log.infof("EDSGER_LOG_LEVEL=%s", log.logLvl.String())

	lockAddr, err = net.ResolveTCPAddr("tcp",
		fmt.Sprintf("127.0.0.1:%d", lockPort))
	if err != nil {
		panic(err)
	}

	ulckAddr, err = net.ResolveTCPAddr("tcp",
		fmt.Sprintf("127.0.0.1:%d", unlockPort))
	if err != nil {
		panic(err)
	}

	// every time this package is loaded into a new Golang process, this init
	// function starts a watchdog goroutine that ensures the server is always
	// running as long as one of this package is loaded anywhere on the system
	go serve()
}

/*
Semaphore is a synchronization object that adheres to all of the same
characteristics as a sync.RWMutex. There are two types of Semaphores: unnamed
and named. Creating an unnamed Semaphore is just the same as creating a
sync.Mutex object:

        // create an unnamed Semaphore
        var sema Semaphore

        // aquire and release a write lock
        sema.Lock()
        sema.Unlock()

        // aquire and release a read lock
        sema.RLock()
        sema.RUnlock()

To create a named Semaphore, simply assign the desired name to the Semaphore's
Name field:

        // create an named Semaphore
        var sema Semaphore
        sema.Name = "MyNamedSemaphore"

More commonly one would use the Go shorthand for assigning field values:

        // create a pointer to a named Semaphore
        sema := &Semaphore{Name: "MyNamedSemaphore"}

All of the above examples are valid ways to create a Semaphore. One thing to
note is that the Name field should never be modified after any of the
Semaphore's methods are invoked. Doing so will put the Semaphore object into an
unstable state, and the object should be discarded.
*/
type Semaphore struct {

	// Name is the name of the Semaphore. It should never be assigned a value
	// after any of the object's synchronization functions have been invoked.
	// Doing so will leave the object in an unstable state.
	Name string

	hash     []byte
	hashOnce sync.Once
	lock     sync.RWMutex
}

// Lock locks the named lock. If the lock is already in use, the calling
// goroutine blocks until the lock is available.
func (s *Semaphore) Lock() {
	s.mustDoLockOp(lockEx)
}

// Unlock unlocks the named lock. It is a run-time error if the named lock
// is not locked on entry to Unlock.
//
// A named lock is not associated with a particular goroutine. It is allowed
// for one goroutine to lock a Mutex and then arrange for another goroutine
// to unlock it.
func (s *Semaphore) Unlock() {
	s.mustDoLockOp(lockEx | lockUn)
}

// RLock locks the named lock for reading.
func (s *Semaphore) RLock() {
	s.mustDoLockOp(lockSh)
}

// RUnlock undoes a single RLock call for the named lock; it does not affect
// other simultaneous readers. It is a run-time error if rw is not locked
// for reading on entry to RUnlock.
func (s *Semaphore) RUnlock() {
	s.mustDoLockOp(lockSh | lockUn)
}

type lockOp byte

const (
	lockSh lockOp = 1 << iota
	lockEx
	_
	lockUn
)

const (
	hshLen = md5.Size
	reqLen = hshLen + 1
	resLen = 1
)

type errno byte

const (
	errSuccess errno = iota
	errUnlockNoLock
	errRUnlockNoLock
)

var errnoMap = map[errno]string{
	errUnlockNoLock:  "edsger: Unlock of unlocked Semaphore",
	errRUnlockNoLock: "edsger: RUnlock of unlocked Semaphore",
}

func (e errno) String() string {
	if e == 0 {
		return ""
	}
	if msg, ok := errnoMap[e]; ok {
		return msg
	}
	return "edsger: Unknown reply from server"
}
func (e errno) Error() string {
	return e.String()
}

type hasRemoteAddr interface {
	RemoteAddr() net.Addr
}

func serve() {

	var (
		isServing     bool
		isServingLock sync.Mutex
	)

	// set up a loop that attempts to create the PID file using the os.O_EXCL
	// flag. if the create attempt files it is because the file already exists
	// and the server is presumably already running. otherwise, if the file is
	// successfully created it means this process should launch the server.
	go func() {
		for {
			f, err := os.OpenFile(pidFilePath, os.O_CREATE|os.O_EXCL, 0644)
			if err != nil {
				time.Sleep(serverWait)
				continue
			}

			isServingLock.Lock()
			isServing = true
			isServingLock.Unlock()

			fmt.Fprintf(f, "%d", os.Getpid())
			f.Close()
			break
		}

		isServing = true

		accept := func(l *listener) {
			for {
				conn, err := l.AcceptTCP()
				if err != nil {
					log.panicf(err)
				}
				go mustHandleRequest(l, conn)
			}
		}

		go accept(mustListen(lockAddr, maxConnections))
		go accept(mustListen(ulckAddr, maxConnections))
	}()

	// set up a loop that looks for the PID file and reads the PID inside of it.
	// if the PID no longer exists on the system then remove the stale PID file.
	// this loop will exit if this process ever becomes the host for the server.
	go func() {
		for {

			isServingLock.Lock()
			if isServing {
				break
			}
			isServingLock.Unlock()

			var (
				pid int
				err error
				f   *os.File
				p   *os.Process
			)

			if f, err = os.Open(pidFilePath); err != nil {
				time.Sleep(serverWait)
				continue
			}

			_, err = fmt.Fscanf(f, "%d", &pid)
			f.Close()

			// in the two branches below that result in the removal of the PID
			// file, please notice that a break is not used. instead the loop
			// iteration is continued. this is because there is no guarantee
			// that even though this process issues the command to remove the
			// PID file that this process will be the one that ends up hosting
			// the server. thus this loop keeps on looping until isServing
			// is flipped to true.

			// if there is an error parsing the PID file then assume the process
			// it once represented no longer exists and remove the PID file
			if err != nil {
				os.Remove(pidFilePath)
				time.Sleep(serverWait)
				log.infof("watchdog: removed stale pid file")
				continue
			}

			// check to see if the process specified by the PID file exists
			if p, err = findProcess(pid); p == nil && err == nil {
				os.Remove(pidFilePath)
				log.infof("watchdog: removed stale pid file")
				time.Sleep(serverWait)
				continue
			}
		}
	}()
}

type cnxn struct {
	l    *listener
	conn *net.TCPConn
	op   lockOp
	hash []byte
	err  errno
}

func mustHandleRequest(l *listener, conn *net.TCPConn) {

	c := &cnxn{
		l:    l,
		conn: conn,
	}

	defer func() {
		c.Close()
		if r := recover(); r != nil {
			panic(r)
		}
	}()

	c.mustHandleRequest()
}

func (c *cnxn) Close() error {
	c.l.CloseTCP(c)
	return nil
}

func (c *cnxn) mustHandleRequest() {

	buf := make([]byte, reqLen)

	if _, err := c.conn.Read(buf); err != nil {
		if err == io.EOF {
			return
		}
		log.panicf(
			"server: laddr=%s, raddr=%s, anum=%d, cnum=%d: read error: %v",
			c.conn.LocalAddr(), c.conn.RemoteAddr(), c.l.anum, c.l.cnum, err)
	}

	c.conn.CloseRead()
	c.op = lockOp(buf[0])
	c.hash = buf[1:]

	log.debugf(
		"server: laddr=%s, raddr=%s, anum=%d, cnum=%d, hash=%x: read success",
		c.conn.LocalAddr(), c.conn.RemoteAddr(), c.l.anum, c.l.cnum, c.hash)

	c.doLockOp()

	reply := []byte{byte(c.err)}
	if _, err := c.conn.Write(reply); err != nil {
		log.panicf(
			"server: laddr=%s, raddr=%s, anum=%d, cnum=%d, "+
				"hash=%x, errno=%v: write error: %v",
			c.conn.LocalAddr(), c.conn.RemoteAddr(),
			c.l.anum, c.l.cnum, c.hash, c.err, err)
	}

	c.conn.CloseWrite()
	log.debugf(
		"server: laddr=%s, raddr=%s, anum=%d, cnum=%d, hash=%x: write success",
		c.conn.LocalAddr(), c.conn.RemoteAddr(), c.l.anum, c.l.cnum, c.hash)
}

func (c *cnxn) doLockOp() {

	var (
		ok   bool
		l    *sync.RWMutex
		name = fmt.Sprintf("%x", c.hash)
	)

	func() {
		wmen.RLock()
		defer wmen.RUnlock()
		l, ok = lcks[name]
	}()

	if !ok {
		l = &sync.RWMutex{}
		func() {
			wmen.Lock()
			defer wmen.Unlock()
			if el, ok := lcks[name]; ok {
				l = el
			} else {
				lcks[name] = l
			}
		}()
	}

	func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}

			if msg, ok := r.(string); ok {

				logErrFunc := func() {
					log.errorf(
						"server: laddr=%s, raddr=%s, anum=%d, cnum=%d, "+
							"hash=%x, op=%d: op err: %v",
						c.conn.LocalAddr(), c.conn.RemoteAddr(),
						c.l.anum, c.l.cnum, c.hash, c.op, msg)
				}

				switch msg {
				case "sync: Unlock of unlocked RWMutex":
					logErrFunc()
					c.err = errUnlockNoLock
					return
				case "sync: RUnlock of unlocked RWMutex":
					logErrFunc()
					c.err = errRUnlockNoLock
					return
				}
			}

			log.panicf(r)
		}()

		log.debugf(
			"server: laddr=%s, raddr=%s, anum=%d, cnum=%d, "+
				"hash=%x, op=%d: op start",
			c.conn.LocalAddr(), c.conn.RemoteAddr(), c.l.anum, c.l.cnum,
			c.hash, c.op)

		switch c.op {
		case lockEx:
			l.Lock()
		case lockEx | lockUn:
			l.Unlock()
		case lockSh:
			l.RLock()
		case lockSh | lockUn:
			l.RUnlock()
		}
	}()

	log.debugf(
		"server: laddr=%s, raddr=%s, anum=%d, cnum=%d, "+
			"hash=%x, op=%d: op success",
		c.conn.LocalAddr(), c.conn.RemoteAddr(), c.l.anum, c.l.cnum,
		c.hash, c.op)
}

type dialFunc func() *net.TCPConn

type dialer struct {
	net.Dialer
	s *Semaphore
}

func newDialer(s *Semaphore) *dialer {
	return &dialer{
		Dialer: net.Dialer{
			Timeout:   time.Duration(1 * time.Millisecond),
			KeepAlive: 0,
		},
		s: s,
	}
}

func (d *dialer) laddr() net.Addr {
	return d.Dialer.LocalAddr
}

func (d *dialer) mustDial(raddr net.Addr) *net.TCPConn {

	var (
		i    int
		err  error
		conn net.Conn
	)

	sleep := func() { time.Sleep(time.Duration(10 * time.Millisecond)) }

	for i = 1; i > 0; i++ {
		log.debugf("client: raddr=%s, name=%s, hash=%x, attempt=%d",
			raddr, d.s.Name, d.s.hash, i)
		if conn, err = d.Dial(proto, raddr.String()); err == nil {
			break
		}
		logErr := func() {
			/*log.errorf("client: raddr=%s, name=%s, hash=%x, attempt=%d: "+
			"error %v",
			raddr, d.s.Name, d.s.hash, i, err)*/
		}
		if strings.Contains(err.Error(), "reset by peer") {
			logErr()
			sleep()
			continue
		}
		if strings.Contains(err.Error(), "i/o timeout") {
			logErr()
			sleep()
			continue
		}
		if strings.Contains(err.Error(), "getsockopt: connection refused") {
			logErr()
			sleep()
			continue
		}
		log.panicf(err)
	}

	log.debugf("client: laddr=%s, raddr=%s, "+
		"name=%s, hash=%x, attempts=%d: success",
		d.laddr(), raddr, d.s.Name, d.s.hash, i)

	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(false)
	return tcpConn
}

func (s *Semaphore) mustDialLock() *net.TCPConn {
	return newDialer(s).mustDial(lockAddr)
}
func (s *Semaphore) mustDialUnlock() *net.TCPConn {
	return newDialer(s).mustDial(ulckAddr)
}

func (s *Semaphore) mustWrite(op lockOp, conn *net.TCPConn, dial dialFunc) {

	var (
		i, n int
		err  error
		buf  = []byte{byte(op)}
	)

	buf = append(buf, s.hash...)

	for i = 1; i > 0; i++ {

		log.debugf("client: laddr=%s, raddr=%s, name=%s, "+
			"hash=%x, op=%d, attempt=%d: write pending",
			conn.LocalAddr(), conn.RemoteAddr(), s.Name,
			s.hash, op, i)

		if n, err = conn.Write(buf); err == nil && n == reqLen {
			//conn.CloseWrite()
			break
		}

		if err != nil {

			logErr := func(err error) {
				/*log.errorf("client: laddr=%s, raddr=%s, name=%s, "+
				"hash=%x, op=%d: write error %v",
				conn.LocalAddr(), conn.RemoteAddr(), s.Name,
				s.hash, op, err)*/
			}
			if strings.Contains(err.Error(), "reset by peer") {
				logErr(err)
				conn = dial()
				continue
			}
			if strings.Contains(err.Error(), "write: broken pipe") {
				logErr(err)
				conn = dial()
				continue
			}
			if strings.Contains(err.Error(), "write: socket is not connected") {
				logErr(err)
				conn = dial()
				continue
			}
		}

		log.panicf(err)
	}

	log.debugf("client: laddr=%s, raddr=%s, name=%s, hash=%x, op=%d, "+
		"attempts=%d: write success",
		conn.LocalAddr(), conn.RemoteAddr(), s.Name, s.hash, op, i)
}

func (s *Semaphore) mustRead(op lockOp, conn *net.TCPConn) error {

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		log.panicf(err)
	}

	//conn.CloseRead()
	result := errno(buf[0])
	log.debugf("client: laddr=%s, raddr=%s, name=%s, "+
		"hash=%x, op=%d, result=%d: read success",
		conn.LocalAddr(), conn.RemoteAddr(), s.Name, s.hash, op, result)

	if result == errSuccess {
		return nil
	}

	return result
}

func (s *Semaphore) hashName() {
	s.hashOnce.Do(func() {
		h := md5.New()
		h.Write([]byte(s.Name))
		s.hash = h.Sum(nil)
	})
}

func (s *Semaphore) mustDoLockOp(op lockOp) {

	// if this is an unnamed Semaphore, do not send the lock operation to the
	// server; handle it locally with the Semaphore's RWMutex object
	if len(s.Name) == 0 {
		switch op {
		case lockEx:
			s.lock.Lock()
		case lockEx | lockUn:
			s.lock.Unlock()
		case lockSh:
			s.lock.RLock()
		case lockSh | lockUn:
			s.lock.RUnlock()
		}
		return
	}

	// hash the Semaphore's name
	s.hashName()

	// get the appropriate dialer function
	dial := s.mustDialLock
	if (op & lockUn) == lockUn {
		dial = s.mustDialUnlock
	}

	conn := dial()

	s.mustWrite(op, conn, dial)

	log.debugf("client: laddr=%s, raddr=%s, name=%s, hash=%x, op=%d: waiting",
		conn.LocalAddr(), conn.RemoteAddr(), s.Name, s.hash, op)

	if err := s.mustRead(op, conn); err != nil {
		log.panicf(err)
	}
}

// RLocker returns a sync.Locker interface that implements the Lock and Unlock
// methods by calling s.RLock and s.RUnlock.
func (s *Semaphore) RLocker() sync.Locker {
	return (*rlocker)(s)
}

type rlocker Semaphore

func (r *rlocker) Lock()   { (*Semaphore)(r).RLock() }
func (r *rlocker) Unlock() { (*Semaphore)(r).RUnlock() }

type listener struct {
	*net.TCPListener
	lock *sync.Cond
	name string
	cnum int
	anum int
	cmax int
}

func mustListen(laddr *net.TCPAddr, maxConn int) *listener {

	l, err := net.ListenTCP(proto, laddr)
	if err != nil {
		log.panicf(err)
	}

	log.infof("server: laddr=%s: now accepting connections", laddr)

	return &listener{
		TCPListener: l,
		lock:        &sync.Cond{L: &sync.Mutex{}},
		cnum:        0,
		anum:        0,
		cmax:        maxConn,
	}
}

func (l *listener) AcceptTCP() (*net.TCPConn, error) {
	l.lock.L.Lock()
	if l.cmax > 0 {
		for l.cnum == l.cmax {
			l.lock.Wait()
		}
	}
	l.anum++
	l.cnum++
	l.lock.L.Unlock()
	conn, err := l.TCPListener.AcceptTCP()
	if err != nil {
		return nil, err
	}
	log.infof("server: laddr=%s, raddr=%s, anum=%d, cnum=%d: accepted conn",
		conn.LocalAddr(), conn.RemoteAddr(), l.anum, l.cnum)
	conn.SetKeepAlive(false)
	return conn, nil
}

func (l *listener) CloseTCP(conn *cnxn) {
	conn.conn.Close()
	l.lock.L.Lock()
	l.cnum--
	l.lock.L.Unlock()
	log.infof("server: laddr=%s, raddr=%s, anum=%d, cnum=%d, "+
		"hash=%x, op=%d, errno=%d: closed conn",
		conn.conn.LocalAddr(), conn.conn.RemoteAddr(),
		l.anum, l.cnum, conn.hash, conn.op, conn.err)
	if l.cmax > 0 {
		l.lock.Signal()
	}
}

func rint(x float64) int {
	if x < 0 {
		return int(math.Ceil(x - 0.5))
	}
	return int(math.Floor(x + 0.5))
}
