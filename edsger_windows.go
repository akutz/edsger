// +build linux darwin

package edsger

import (
	"os"
)

func findProcess(pid int) (*os.Process, error) {
	return os.FindProcess(pid)
}
