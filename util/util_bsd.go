//go:build darwin || freebsd || netbsd || openbsd

package util

import (
	"os/exec"
	"syscall"
	"unsafe"
)

func isTTY(fd uintptr) bool {
	var termios syscall.Termios
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, fd, syscall.TIOCGETA, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return err == 0
}

func EnsureChildShutdown(cmd *exec.Cmd, sig int) {
	// This ensures that, on Linux, if Faktory panics, our Redis child process will immediately
	// get a SIGTERM signal to shutdown.  No such feature on Darwin/BSD, Redis will orphan.
}
