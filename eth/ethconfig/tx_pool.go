package ethconfig

import (
	"errors"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"time"
)

var (
	// ErrAlreadyKnown is returned if the transactions is already contained
	// within the pool.
	ErrAlreadyKnown = errors.New("already known")

	// ErrInvalidSender is returned if the transaction contains an invalid signature.
	ErrInvalidSender = errors.New("invalid sender")

	// ErrUnderpriced is returned if a transaction's gas price is below the minimum
	// configured for the transaction pool.
	ErrUnderpriced = errors.New("transaction underpriced")

	// ErrTxPoolOverflow is returned if the transaction pool is full and can't accpet
	// another remote transaction.
	ErrTxPoolOverflow = errors.New("txpool is full")

	// ErrReplaceUnderpriced is returned if a transaction is attempted to be replaced
	// with a different one without the required price bump.
	ErrReplaceUnderpriced = errors.New("replacement transaction underpriced")

	// ErrGasLimit is returned if a transaction's requested gas limit exceeds the
	// maximum allowance of the current block.
	ErrGasLimit = errors.New("exceeds block gas limit")

	// ErrNegativeValue is a sanity error to ensure no one is able to specify a
	// transaction with a negative value.
	ErrNegativeValue = errors.New("negative value")

	// ErrOversizedData is returned if the input data of a transaction is greater
	// than some meaningful limit a user might use. This is not a consensus error
	// making the transaction invalid, rather a DOS protection.
	ErrOversizedData = errors.New("oversized data")
)

// TxPoolConfig are the configuration parameters of the transaction pool.
type TxPoolConfig struct {
	Disable  bool
	Locals   []libcommon.Address // Addresses that should be treated by default as local
	NoLocals bool             // Whether local transaction handling should be disabled

	PriceLimit uint64 // Minimum gas price to enforce for acceptance into the pool
	PriceBump  uint64 // Minimum price bump percentage to replace an already existing transaction (nonce)

	AccountSlots uint64 // Number of executable transaction slots guaranteed per account
	GlobalSlots  uint64 // Maximum number of executable transaction slots for all accounts
	AccountQueue uint64 // Maximum number of non-executable transaction slots permitted per account
	GlobalQueue  uint64 // Maximum number of non-executable transaction slots for all accounts

	GlobalBaseFeeQueue uint64 // Maximum number of non-executable transaction slots for all accounts

	Lifetime      time.Duration // Maximum amount of time non-executable transaction are queued
	StartOnInit   bool
	TracedSenders []string // List of senders for which tx pool should print out debugging info
}

// DeprecatedDefaultTxPoolConfig contains the default configurations for the transaction
// pool.
var DeprecatedDefaultTxPoolConfig = TxPoolConfig{
	PriceLimit: 1,
	PriceBump:  10,

	AccountSlots:       16,
	GlobalSlots:        10_000,
	GlobalBaseFeeQueue: 30_000,
	AccountQueue:       64,
	GlobalQueue:        30_000,

	Lifetime: 3 * time.Hour,
}

var DefaultTxPool2Config = func(pool1Cfg TxPoolConfig) txpoolcfg.Config {
	cfg := txpoolcfg.DefaultConfig
	cfg.PendingSubPoolLimit = int(pool1Cfg.GlobalSlots)
	cfg.BaseFeeSubPoolLimit = int(pool1Cfg.GlobalBaseFeeQueue)
	cfg.QueuedSubPoolLimit = int(pool1Cfg.GlobalQueue)
	cfg.PriceBump = pool1Cfg.PriceBump
	cfg.MinFeeCap = pool1Cfg.PriceLimit
	cfg.AccountSlots = pool1Cfg.AccountSlots
	cfg.LogEvery = 1 * time.Minute
	cfg.CommitEvery = 5 * time.Minute
	cfg.TracedSenders = pool1Cfg.TracedSenders
	return cfg
}
