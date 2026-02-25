//go:build !linux

package worker

func EnableParentDeathSignal() error {
	return nil
}
