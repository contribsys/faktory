// +build linux

package util

import (
	"syscall"
	"unsafe"
)

func isTTY(fd int) bool {
	var termios syscall.Termios
	// syscall.TCGETS
	// https://groups.google.com/forum/#!topic/golang-checkins/kieURujjDEk
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), 0x5401, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return err == 0
}
