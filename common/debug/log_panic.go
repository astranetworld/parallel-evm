package debug

import (
	"os"
	"starlink-world/erigon-evm/common2/dbg"
	"starlink-world/erigon-evm/log"
	"syscall"
)

var sigc chan os.Signal

func GetSigC(sig *chan os.Signal) {
	sigc = *sig
}

// LogPanic - does log panic to logger and to <datadir>/crashreports then stops the process
func LogPanic() {
	panicResult := recover()
	if panicResult == nil {
		return
	}

	log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
	if sigc != nil {
		sigc <- syscall.SIGINT
	}
}
