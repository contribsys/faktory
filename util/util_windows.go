package util

import (
	"os/exec"
)

func isTTY(fd uintptr) bool {
	// this function controls if we output ANSI coloring to the terminal.
	// dunno how to do this on Windows so just play safe and assume it is not a TTY
	return false
}

func EnsureChildShutdown(cmd *exec.Cmd, sig int) {
	// This ensures that, on Linux, if Faktory panics, the child process will immediately
	// get a signal.  Dunno if this is possible on Windows or how it will behave.
}
