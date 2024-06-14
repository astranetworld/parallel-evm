package stage

import (
	"bytes"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core/state/historyv2read"
	"starlink-world/erigon-evm/core/types/accounts"
	"starlink-world/erigon-evm/crypto"
)

type StateReader struct {
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort
	blockNr                      uint64
	tx                           kv.Tx
	db                           kv.Getter
}

func NewStateHistoryReader(tx kv.Tx, db kv.Getter, blockNr uint64) *StateReader {
	c1, _ := tx.Cursor(kv.AccountsHistory)
	c2, _ := tx.Cursor(kv.StorageHistory)
	c3, _ := tx.CursorDupSort(kv.AccountChangeSet)
	c4, _ := tx.CursorDupSort(kv.StorageChangeSet)
	return &StateReader{
		tx:          tx,
		blockNr:     blockNr,
		db:          db,
		accHistoryC: c1, storageHistoryC: c2, accChangesC: c3, storageChangesC: c4,
	}
}

func (dbr *StateReader) SetBlockNumber(blockNr uint64) {
	dbr.blockNr = blockNr
}

func (dbr *StateReader) GetOne(bucket string, key []byte) ([]byte, error) {
	if len(bucket) == 0 {
		return nil, nil
	}
	return dbr.db.GetOne(bucket, key[:])
}

func (r *StateReader) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	v, err := r.db.GetOne(kv.PlainState, address[:])
	if err == nil && len(v) > 0 {
		var acc accounts.Account
		if err := acc.DecodeForStorage(v); err != nil {
			return nil, err
		}
		return &acc, nil
	}
	enc, err := historyv2read.GetAsOf(r.tx, r.accHistoryC, r.accChangesC, false /* storage */, address[:], r.blockNr)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (r *StateReader) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	v, err := r.db.GetOne(kv.PlainState, compositeKey)
	if err == nil && len(v) > 0 {
		return v, nil
	}
	return historyv2read.GetAsOf(r.tx, r.storageHistoryC, r.storageChangesC, true /* storage */, compositeKey, r.blockNr)
}

func (r *StateReader) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], crypto.Keccak256(nil)) {
		return nil, nil
	}
	var val []byte
	v, err := r.tx.GetOne(kv.Code, codeHash[:])
	if err != nil || len(v) == 0 {
		panic(err)
		return nil, err
	}
	val = common.CopyBytes(v)
	return val, nil
}

func (r *StateReader) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (r *StateReader) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}
