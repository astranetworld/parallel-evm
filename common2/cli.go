package common

import (
	"context"
	"os"
	"os/signal"
	"starlink-world/erigon-evm/log"
	"syscall"
)

func RootContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()

		ch := make(chan os.Signal, 1)
		defer close(ch)

		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case sig := <-ch:
			log.Info("Got interrupt, shutting down...", "sig", sig)
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
