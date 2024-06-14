package common

import "errors"

var ErrStopped = errors.New("stopped")
var ErrUnwind = errors.New("unwound")

func Stopped(ch <-chan struct{}) error {
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return ErrStopped
	default:
	}
	return nil
}

func SafeClose(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
		// Channel was already closed
	default:
		close(ch)
	}
}

// PrioritizedSend message to channel, but if channel is full (slow consumer) - drop half of old messages (not new)
func PrioritizedSend[t any](ch chan t, msg t) {
	select {
	case ch <- msg:
	default: //if channel is full (slow consumer), drop old messages (not new)
		for i := 0; i < cap(ch)/2; i++ {
			select {
			case <-ch:
			default:
			}
		}
		ch <- msg
	}
}
