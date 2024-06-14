package state

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	historyv22 "github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"golang.org/x/exp/maps"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core/types/accounts"
	"sync"
)

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriterV3 struct {
	accountChanges map[libcommon.Address][]byte
	storageChanged map[libcommon.Address]bool
	storageChanges map[string][]byte
	blockNumber    uint64
}

//func NewChangeSetWriterV3() *ChangeSetWriter {
//	return &ChangeSetWriter{
//		accountChanges: make(map[libcommon.Address][]byte),
//		storageChanged: make(map[libcommon.Address]bool),
//		storageChanges: make(map[string][]byte),
//	}
//}

var ChangeSetWriterV3Pool = sync.Pool{New: func() any {
	return &ChangeSetWriterV3{
		accountChanges: make(map[libcommon.Address][]byte),
		storageChanged: make(map[libcommon.Address]bool),
		storageChanges: make(map[string][]byte),
	}
}}

func NewChangeSetWriterPlainV3(blockNumber uint64) *ChangeSetWriterV3 {
	//return &ChangeSetWriterV3{
	//	accountChanges: make(map[libcommon.Address][]byte),
	//	storageChanged: make(map[libcommon.Address]bool),
	//	storageChanges: make(map[string][]byte),
	//	blockNumber:    blockNumber,
	//}
	csw := ChangeSetWriterV3Pool.Get().(*ChangeSetWriterV3)
	maps.Clear(csw.accountChanges)
	maps.Clear(csw.storageChanged)
	maps.Clear(csw.storageChanges)
	csw.blockNumber = blockNumber
	return csw
}

func ReturnChangeSetWriterPlainV3(v3 *ChangeSetWriterV3) {
	if v3 == nil {
		return
	}
	ChangeSetWriterV3Pool.Put(v3)
}

func (w *ChangeSetWriterV3) GetAccountChanges() (*historyv22.ChangeSet, error) {
	cs := historyv22.NewAccountChangeSet()
	for address, val := range w.accountChanges {
		if err := cs.Add(common.CopyBytes(address[:]), val); err != nil {
			return nil, err
		}
	}
	return cs, nil
}
func (w *ChangeSetWriterV3) GetStorageChanges() (*historyv22.ChangeSet, error) {
	cs := historyv22.NewStorageChangeSet()
	for key, val := range w.storageChanges {
		if err := cs.Add([]byte(key), val); err != nil {
			return nil, err
		}
	}
	return cs, nil
}

//func accountsEqual(a1, a2 *accounts.Account) bool {
//	if a1.Nonce != a2.Nonce {
//		return false
//	}
//	if !a1.Initialised {
//		if a2.Initialised {
//			return false
//		}
//	} else if !a2.Initialised {
//		return false
//	} else if a1.Balance.Cmp(&a2.Balance) != 0 {
//		return false
//	}
//	if a1.IsEmptyCodeHash() {
//		if !a2.IsEmptyCodeHash() {
//			return false
//		}
//	} else if a2.IsEmptyCodeHash() {
//		return false
//	} else if a1.CodeHash != a2.CodeHash {
//		return false
//	}
//	return true
//}

func (w *ChangeSetWriterV3) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	//fmt.Printf("balance,%x,%d\n", address, &account.Balance)
	if !accountsEqual(original, account) || w.storageChanged[address] {
		w.accountChanges[address] = originalAccountData(original, true /*omitHashes*/)
	}
	return nil
}

func (w *ChangeSetWriterV3) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	//fmt.Printf("code,%x,%x\n", address, code)
	return nil
}

func (w *ChangeSetWriterV3) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	//fmt.Printf("delete,%x\n", address)
	if original == nil || !original.Initialised {
		return nil
	}
	w.accountChanges[address] = originalAccountData(original, false)
	return nil
}

func (w *ChangeSetWriterV3) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	//fmt.Printf("storage,%x,%x,%x\n", address, *key, value.Bytes())
	if *original == *value {
		return nil
	}

	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())

	w.storageChanges[string(compositeKey)] = original.Bytes()
	w.storageChanged[address] = true

	return nil
}

func (w *ChangeSetWriterV3) CreateContract(address libcommon.Address) error {
	return nil
}

func UniqChangeSet(changeSet *historyv22.ChangeSet) {
	mp := make(map[string][]byte, changeSet.Len())
	for _, ch := range changeSet.Changes {
		mp[string(ch.Key)] = ch.Value
	}
	changeSet.Changes = changeSet.Changes[:0]
	for k, v := range mp {
		changeSet.Changes = append(changeSet.Changes, historyv22.Change{
			Key:   []byte(k),
			Value: v,
		})
	}
}

func RecoverChangeSet(mp map[string][]byte, isStorage bool) (*historyv22.ChangeSet, error) {
	var cs *historyv22.ChangeSet
	if isStorage {
		cs = historyv22.NewStorageChangeSet()
	} else {
		cs = historyv22.NewAccountChangeSet()
	}

	for k, v := range mp {
		if err := cs.Add([]byte(k), v); nil != err {
			return nil, err
		}
	}

	return cs, nil
}

//func (w *ChangeSetWriterV3) WriteChangeSets() error {
//	accountChanges, err := w.GetAccountChanges()
//	if err != nil {
//		return err
//	}
//	// k  blockNumber
//	// V  address + accountEncode
//	if err = historyv22.Mapper[kv.AccountChangeSet].Encode(w.blockNumber, accountChanges, func(k, v []byte) error {
//		if err = w.db.AppendDup(kv.AccountChangeSet, k, v); err != nil {
//			return err
//		}
//		return nil
//	}); err != nil {
//		return err
//	}
//
//	storageChanges, err := w.GetStorageChanges()
//	if err != nil {
//		return err
//	}
//	if storageChanges.Len() == 0 {
//		return nil
//	}
//	// k  block + address + incarnation
//	// v  key + value
//	if err = historyv22.Mapper[kv.StorageChangeSet].Encode(w.blockNumber, storageChanges, func(k, v []byte) error {
//		if err = w.db.AppendDup(kv.StorageChangeSet, k, v); err != nil {
//			return err
//		}
//		return nil
//	}); err != nil {
//		return err
//	}
//	return nil
//}
//
//func (w *ChangeSetWriterV3) WriteHistory() error {
//	accountChanges, err := w.GetAccountChanges()
//	if err != nil {
//		return err
//	}
//	err = writeIndex(w.blockNumber, accountChanges, kv.AccountsHistory, w.db)
//	if err != nil {
//		return err
//	}
//
//	storageChanges, err := w.GetStorageChanges()
//	if err != nil {
//		return err
//	}
//	err = writeIndex(w.blockNumber, storageChanges, kv.StorageHistory, w.db)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
