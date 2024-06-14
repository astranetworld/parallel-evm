package common

import (
	"bytes"
	"encoding/hex"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"starlink-world/erigon-evm/common/hexutil"
)

// Hashes is a slice of common.Hash, implementing sort.Interface
type Hashes []libcommon.Hash

func (hashes Hashes) Len() int {
	return len(hashes)
}
func (hashes Hashes) Less(i, j int) bool {
	return bytes.Compare(hashes[i][:], hashes[j][:]) == -1
}
func (hashes Hashes) Swap(i, j int) {
	hashes[i], hashes[j] = hashes[j], hashes[i]
}

// Addresses is a slice of common.Address, implementing sort.Interface
type Addresses []libcommon.Address

func (addrs Addresses) Len() int {
	return len(addrs)
}
func (addrs Addresses) Less(i, j int) bool {
	return bytes.Compare(addrs[i][:], addrs[j][:]) == -1
}
func (addrs Addresses) Swap(i, j int) {
	addrs[i], addrs[j] = addrs[j], addrs[i]
}

const StorageKeyLen = 2*length.Hash + length.Incarnation

// StorageKey is representation of address of a contract storage item
// It consists of two parts, each of which are 32-byte hashes:
// 1. Hash of the contract's address
// 2. Hash of the item's key
type StorageKey [StorageKeyLen]byte

// StorageKeys is a slice of StorageKey, implementing sort.Interface
type StorageKeys []StorageKey

func (keys StorageKeys) Len() int {
	return len(keys)
}
func (keys StorageKeys) Less(i, j int) bool {
	return bytes.Compare(keys[i][:], keys[j][:]) == -1
}
func (keys StorageKeys) Swap(i, j int) {
	keys[i], keys[j] = keys[j], keys[i]
}

// UnprefixedAddress allows marshaling an Address without 0x prefix.
type UnprefixedAddress libcommon.Address

// UnmarshalText decodes the address from hex. The 0x prefix is optional.
func (a *UnprefixedAddress) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedUnprefixedText("UnprefixedAddress", input, a[:])
}

// MarshalText encodes the address as hex.
func (a UnprefixedAddress) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(a[:])), nil
}