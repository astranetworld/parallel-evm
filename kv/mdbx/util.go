package mdbx

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	mdbxbind "github.com/torquem-ch/mdbx-go/mdbx"
	"starlink-world/erigon-evm/log"
)

func MustOpen(path string) kv.RwDB {
	db, err := Open(path, log.New(), false)
	if err != nil {
		panic(err)
	}
	return db
}

func MustOpenRo(path string) kv.RoDB {
	db, err := Open(path, log.New(), true)
	if err != nil {
		panic(err)
	}
	return db
}

// Open - main method to open database.
func Open(path string, logger log.Logger, readOnly bool) (kv.RwDB, error) {
	var db kv.RwDB
	var err error
	opts := NewMDBX(logger).Path(path)
	if readOnly {
		opts = opts.Flags(func(flags uint) uint { return flags | mdbxbind.Readonly })
	}
	db, err = opts.Open()

	if err != nil {
		return nil, err
	}
	return db, nil
}
