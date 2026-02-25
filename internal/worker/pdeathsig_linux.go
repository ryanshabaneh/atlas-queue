//go:build linux

package worker

import "syscall"

const prSetPDeathSig = 1

// EnableParentDeathSignal asks the kernel to deliver SIGTERM to this process
// when its direct parent exits. This closes the signal-forwarding gap that can
// occur when the worker is launched via wrapper processes (for example `go run`).
func EnableParentDeathSignal() error {
	_, _, errno := syscall.RawSyscall(
		syscall.SYS_PRCTL,
		uintptr(prSetPDeathSig),
		uintptr(syscall.SIGTERM),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}
