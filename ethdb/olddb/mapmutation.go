package olddb

import (
	"context"
	"encoding/binary"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"sort"
	"starlink-world/erigon-evm/crypto"
	"sync"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"starlink-world/erigon-evm/ethdb"
	"starlink-world/erigon-evm/log"
)

type Mapmutation struct {
	puts    map[string]map[string][]byte // table -> key -> value ie. blocks -> hash -> blockBod
	records map[string]struct{}
	db      kv.RwTx
	quit    <-chan struct{}
	clean   func()
	mu      sync.RWMutex
	size    int
	count   uint64
	tmpdir  string
}

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewHashBatch(tx kv.RwTx, quit <-chan struct{}, tmpdir string) *Mapmutation {
	clean := func() {}
	if quit == nil {
		ch := make(chan struct{})
		clean = func() { close(ch) }
		quit = ch
	}

	return &Mapmutation{
		db:     tx,
		puts:   make(map[string]map[string][]byte),
		quit:   quit,
		clean:  clean,
		tmpdir: tmpdir,
	}
}

func (m *Mapmutation) Puts() map[string]map[string][]byte {
	return m.puts
}

func (m *Mapmutation) RwKV() kv.RwDB {
	if casted, ok := m.db.(ethdb.HasRwKV); ok {
		return casted.RwKV()
	}
	return nil
}
func (m *Mapmutation) StartRecord() {
	if m.records == nil {
		m.records = make(map[string]struct{}, 1000)
	} else {
		for k := range m.records {
			delete(m.records, k)
		}
	}
}

func (m *Mapmutation) getMem(table string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.puts[table]; !ok {
		return nil, false
	}
	if value, ok := m.puts[table][*(*string)(unsafe.Pointer(&key))]; ok {
		return value, ok
	}

	return nil, false
}

func (m *Mapmutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	if err = m.Put(kv.Sequence, []byte(bucket), newVBytes); err != nil {
		return 0, err
	}

	return currentV, nil
}
func (m *Mapmutation) ReadSequence(bucket string) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}
	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

// Can only be called from the worker thread
func (m *Mapmutation) GetOne(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		return value, nil
	}
	if m.db != nil {
		// TODO: simplify when tx can no longer be parent of mutation
		value, err := m.db.GetOne(table, key)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

// Can only be called from the worker thread
func (m *Mapmutation) Get(table string, key []byte) ([]byte, error) {
	value, err := m.GetOne(table, key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ethdb.ErrKeyNotFound
	}

	return value, nil
}

func (m *Mapmutation) Last(table string) ([]byte, []byte, error) {
	c, err := m.db.Cursor(table)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()
	return c.Last()
}

func (m *Mapmutation) Print() {
	for k, v := range m.puts {
		fmt.Println("k", k)
		for k1, v1 := range v {
			fmt.Println(hexutility.Encode(*(*[]byte)(unsafe.Pointer(&k1))), hexutility.Encode(v1))
		}
	}
}

func (m *Mapmutation) Hash() (h libcommon.Hash) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st := crypto.NewKeccakState()
	v := m.puts[kv.PlainState]
	k2s := make(sort.StringSlice, 0, len(v))
	if m.records == nil {
		for k2 := range v {
			k2s = append(k2s, k2)
		}
	} else {
		for k2 := range m.records {
			k2s = append(k2s, k2)
		}
	}
	sort.Sort(k2s)
	for _, k2 := range k2s {
		k := k2
		st.Write(*(*[]byte)(unsafe.Pointer(&k)))
		st.Write(v[k2])
	}
	st.Read(h[:])
	return h
}

type KV struct {
	K []byte
	V []byte
}

func (m *Mapmutation) HashMap() []*KV {
	m2 := m.puts[kv.PlainState]
	if len(m2) == 0 {
		return nil
	}
	v := m.puts[kv.PlainState]
	k2s := make(sort.StringSlice, 0, len(v)+1)
	if m.records == nil {
		for k2 := range v {
			k2s = append(k2s, k2)
		}
	} else {
		for k2 := range m.records {
			k2s = append(k2s, k2)
		}
	}
	sort.Sort(k2s)
	m1 := make([]*KV, 0, len(m2))
	for _, k1 := range k2s {
		h := crypto.Keccak256Hash([]byte(k1))
		v := m2[k1]
		if len(v) == length.Hash {
			m1 = append(m1, &KV{K: h[:], V: v})
		} else {
			vv := crypto.Keccak256Hash(v)
			m1 = append(m1, &KV{K: h[:], V: vv[:]})
		}
	}
	return m1
}

func (m *Mapmutation) Hash2() (h libcommon.Hash) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st := crypto.NewKeccakState()
	v := m.puts[kv.PlainState]
	k2s := make(sort.StringSlice, 0, len(v))
	if m.records == nil {
		for k2 := range v {
			k2s = append(k2s, k2)
		}
	} else {
		for k2 := range m.records {
			k2s = append(k2s, k2)
		}
	}
	sort.Sort(k2s)
	for _, k2 := range k2s {
		k := k2
		st.Write(*(*[]byte)(unsafe.Pointer(&k)))
		st.Write(v[k2])
		fmt.Println(hexutility.Encode(*(*[]byte)(unsafe.Pointer(&k))), hexutility.Encode(v[k2]))
	}
	st.Read(h[:])
	return h
}

func (m *Mapmutation) Has(table string, key []byte) (bool, error) {
	if _, ok := m.getMem(table, key); ok {
		return ok, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
	}
	return false, nil
}

func (m *Mapmutation) Put(table string, k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.puts[table]; !ok {
		m.puts[table] = make(map[string][]byte)
	}

	stringKey := string(k)

	var ok bool
	if _, ok = m.puts[table][stringKey]; !ok {
		m.size += len(v) - len(m.puts[table][stringKey])
		m.puts[table][stringKey] = v
		return nil
	}
	m.puts[table][stringKey] = v
	m.size += len(k) + len(v)
	m.count++
	return nil
}

func (m *Mapmutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *Mapmutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *Mapmutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Mapmutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *Mapmutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForPrefix(bucket, prefix, walker)
}

func (m *Mapmutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *Mapmutation) Delete(table string, k []byte) error {
	return m.Put(table, k, nil)
}

func (m *Mapmutation) doCommit(tx kv.RwTx) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	count := 0
	total := float64(m.count)
	for table, bucket := range m.puts {
		collector := etl.NewCollector("", m.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer collector.Close()
		for key, value := range bucket {
			collector.Collect([]byte(key), value)
			count++
			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
				log.Info("Write to db", "progress", progress, "current table", table)
				tx.CollectMetrics()
			}
		}
		if err := collector.Load(m.db, table, etl.IdentityLoadFunc, etl.TransformArgs{Quit: m.quit}); err != nil {
			return err
		}
	}

	tx.CollectMetrics()
	return nil
}

func (m *Mapmutation) Commit() error {
	if m.db == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.doCommit(m.db); err != nil {
		return err
	}

	m.puts = map[string]map[string][]byte{}
	m.size = 0
	m.count = 0
	m.clean()
	return nil
}

func (m *Mapmutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = map[string]map[string][]byte{}
	m.size = 0
	m.count = 0
	m.size = 0
	m.clean()
}

func (m *Mapmutation) Close() {
	m.Rollback()
}

func (m *Mapmutation) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	panic("mutation can't start transaction, because doesn't own it")
}

func (m *Mapmutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *Mapmutation) SetRwKV(kv kv.RwDB) {
	hasRwKV, ok := m.db.(ethdb.HasRwKV)
	if !ok {
		log.Warn("Failed to convert Mapmutation type to HasRwKV interface")
	}
	hasRwKV.SetRwKV(kv)
}
