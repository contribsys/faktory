//go:build linux

package util

import (
	"os/exec"
	"syscall"
	"unsafe"
)

func isTTY(fd uintptr) bool {
	var termios syscall.Termios
	// syscall.TCGETS
	// https://groups.google.com/forum/#!topic/golang-checkins/kieURujjDEk
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, fd, 0x5401, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return err == 0
}

func EnsureChildShutdown(cmd *exec.Cmd, sig int) {
	// This ensures that, on Linux, if Faktory panics, our child process will immediately
	// get a SIGTERM signal to shutdown.  No such feature on Darwin/BSD, child will orphan.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.Signal(sig),
	}
}
