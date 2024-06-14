package state

import (
	"bytes"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core/types/accounts"
	"starlink-world/erigon-evm/rlp"
	"unsafe"
)

type GetOneFun func(table string, key []byte) ([]byte, error)

type Item struct {
	Key   []byte
	Value []byte
}
type Items []*Item

func (s Items) Len() int           { return len(s) }
func (s Items) Less(i, j int) bool { return bytes.Compare(s[i].Key, s[j].Key) < 0 }
func (s Items) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type RangeItems []Items

type Snapshot struct {
	Items     Items
	OutHash   libcommon.Hash
	accounts  map[string]int
	storage   map[string]int
	written   bool
	getOneFun GetOneFun
}

func NewWritableSnapshot() *Snapshot {
	return &Snapshot{Items: make([]*Item, 0), written: true, OutHash: emptyCodeHashH, accounts: make(map[string]int), storage: make(map[string]int)}
}

func NewReadableSnapshot() *Snapshot {
	return &Snapshot{Items: make([]*Item, 0), written: false, OutHash: emptyCodeHashH, accounts: make(map[string]int), storage: make(map[string]int)}
}

func (s *Snapshot) SetGetFun(f GetOneFun) {
	s.getOneFun = f
}

func (s *Snapshot) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	if s.written {
		return nil, nil
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	index, ok := s.storage[*(*string)(unsafe.Pointer(&compositeKey))]
	if !ok {
		if s.getOneFun != nil {
			return s.getOneFun(kv.PlainState, compositeKey)
		}
		return nil, nil
	}
	if s.Items[index] == nil {
		return nil, nil
	}
	return s.Items[index].Value, nil
}

func (s *Snapshot) CanWrite() bool {
	return s.written
}

func ReadSnapshotData(data []byte) (*Snapshot, error) {
	snap := NewReadableSnapshot()
	if len(data) == 0 {
		return snap, nil
	}
	if err := rlp.DecodeBytes(data, &snap); err != nil {
		return nil, err
	}
	for k, v := range snap.Items {
		if len(v.Key) == length.Addr {
			snap.accounts[*(*string)(unsafe.Pointer(&v.Key))] = k
		} else {
			snap.storage[*(*string)(unsafe.Pointer(&v.Key))] = k
		}
	}
	return snap, nil
}

func (s *Snapshot) SetHash(hash libcommon.Hash) {
	if s.written {
		return
	}
	s.OutHash = hash
}

func (s *Snapshot) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	if s.written {
		return nil, nil
	}
	addr := address[:]
	index, ok := s.accounts[*(*string)(unsafe.Pointer(&addr))]
	if !ok {
		if s.getOneFun != nil {
			v, err := s.getOneFun(kv.PlainState, address[:])
			if err != nil {
				return nil, err
			}
			if v == nil {
				return nil, nil
			}
			var acc accounts.Account
			if err := acc.DecodeForStorage(v); err != nil {
				return nil, err
			}
			return &acc, nil
		}
		return nil, nil
	}
	if s.Items[index] == nil {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(s.Items[index].Value); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (s *Snapshot) AddAccount(address libcommon.Address, account *accounts.Account) {
	if !s.written || account == nil {
		return
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	s.Items = append(s.Items, &Item{Key: address[:], Value: value})
	ss := address[:]
	//fmt.Println("address", address.Hex())
	s.accounts[*(*string)(unsafe.Pointer(&ss))] = len(s.Items)
}

func (s *Snapshot) AddStorage(address libcommon.Address, key *libcommon.Hash, incarnation uint64, value []byte) {
	if !s.written || len(value) == 0 {
		return
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	s.Items = append(s.Items, &Item{Key: compositeKey, Value: value})
	//fmt.Println("address", address.Hex(), key.Hex())
	s.storage[*(*string)(unsafe.Pointer(&compositeKey))] = len(s.storage)
}
