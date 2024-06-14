package mdbx

import (
	"errors"
	"fmt"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"starlink-world/erigon-evm/common/dbutils"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"starlink-world/erigon-evm/core/rawdb"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/params"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/tsdb/fileutil"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/ethdb"
	"starlink-world/erigon-evm/log"
)

var (
	// errReadOnly is returned if the freezer is opened in read only mode. All the
	// mutations are disallowed.
	errReadOnly = errors.New("read only")

	errNoWriteBusiness = errors.New("no write business")

	errRepeatedWriteBusiness = errors.New("repeated write business")

	// errUnknownTable is returned if the user attempts to read from a table that is
	// not tracked by the freezer.
	errUnknownTable = errors.New("unknown table")

	// errOutOrderInsertion is returned if the user attempts to inject out-of-order
	// binary blobs into the freezer.
	errOutOrderInsertion = errors.New("the append operation is out-order")

	// errSymlinkDatadir is returned if the ancient directory specified by user
	// is a symbolic link.
	errSymlinkDatadir = errors.New("symbolic link datadir is not supported")
)

const (
	// freezerRecheckInterval is the frequency to check the key-value database for
	// chain progression that might permit new blocks to be frozen into immutable
	// storage.
	freezerRecheckInterval = time.Minute

	// freezerBatchLimit is the maximum number of blocks to freeze in one batch
	// before doing an fsync and deleting it from the key-value store.
	freezerBatchLimit = 30000

	// freezerTableSize defines the maximum size of freezer data files.
	freezerTableSize = 2 * 1000 * 1000 * 1000
)

// freezer is a memory mapped append-only database to store immutable chain data
// into flat files:
//
//   - The append only nature ensures that disk writes are minimized.
//   - The memory mapping ensures we can max out system memory for caching without
//     reserving it for go-ethereum. This would also reduce the memory requirements
//     of Geth, and thus also GC overhead.
type freezer struct {
	// WARNING: The `frozen` field is accessed atomically. On 32 bit platforms, only
	// 64-bit aligned fields can be atomic. The struct is guaranteed to be so aligned,
	// so take advantage of that (https://golang.org/pkg/sync/atomic/#pkg-note-BUG).
	frozen    uint64 // Number of blocks already frozen
	tail      uint64 // Number of the first stored item in the freezer
	threshold uint64 // Number of recent blocks not to freeze (params.FullImmutabilityThreshold apart from tests)

	// This lock synchronizes writers and the truncate operation, as well as
	// the "atomic" (batched) read operations.
	writeLock  sync.RWMutex
	writeBatch *freezerBatch
	newRWTx    bool

	readonly     bool
	tables       map[string]*freezerTable // Data tables for storing everything
	instanceLock fileutil.Releaser        // File-system lock to prevent double opens

	trigger chan chan struct{} // Manual blocking freeze trigger, test determinism

	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// newFreezer creates a chain freezer that moves ancient chain data into
// append-only flat file containers.
//
// The 'tables' argument defines the data tables. If the value of a map
// entry is true, snappy compression is disabled for the table.
func newFreezer(datadir string, readonly bool, maxTableSize uint32, tables map[string]bool) (*freezer, error) {
	// Create the initial baseFreezer object
	// Ensure the datadir is not a symbolic link if it exists.
	if info, err := os.Lstat(datadir); !os.IsNotExist(err) {
		if info.Mode()&os.ModeSymlink != 0 {
			log.Warn("Symbolic link ancient database is not supported", "path", datadir)
			return nil, errSymlinkDatadir
		}
	}
	// Leveldb uses LOCK as the filelock filename. To prevent the
	// name collision, we use FLOCK as the lock name.
	lock, _, err := fileutil.Flock(filepath.Join(datadir, "FLOCK"))
	if err != nil {
		return nil, err
	}
	// Open all the supported data tables
	freezer := &freezer{
		readonly:     readonly,
		threshold:    params.FullImmutabilityThreshold,
		tables:       make(map[string]*freezerTable),
		instanceLock: lock,
		trigger:      make(chan chan struct{}),
		quit:         make(chan struct{}),
	}

	// Create the tables.
	for name, disableSnappy := range tables {
		table, err := newTable(datadir, name, maxTableSize, disableSnappy, readonly)
		if err != nil {
			for _, table := range freezer.tables {
				table.Close()
			}
			lock.Release()
			return nil, err
		}
		freezer.tables[name] = table
	}

	if freezer.readonly {
		// In readonly mode only validate, don't truncate.
		// validate also sets `baseFreezer.frozen`.
		err = freezer.validate()
	} else {
		// Truncate all tables to common length.
		err = freezer.repair()
	}
	if err != nil {
		for _, table := range freezer.tables {
			table.Close()
		}
		lock.Release()
		return nil, err
	}

	// Create the write batch.
	freezer.writeBatch = newFreezerBatch(freezer)

	log.Info("Opened ancient database", "database", datadir, "readonly", readonly)
	return freezer, nil
}

// Close terminates the chain baseFreezer, unmapping all the data files.
func (f *freezer) Close() error {
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	var errs []error
	f.closeOnce.Do(func() {
		close(f.quit)
		// Wait for any background freezing to stop
		f.wg.Wait()
		for _, table := range f.tables {
			if err := table.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if err := f.instanceLock.Release(); err != nil {
			errs = append(errs, err)
		}
	})
	if errs != nil {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// HasAncient returns an indicator whether the specified ancient data exists
// in the baseFreezer.
func (f *freezer) HasAncient(kind string, number uint64) (bool, error) {
	if table := f.tables[kind]; table != nil {
		return table.has(number), nil
	}
	return false, nil
}

// Ancient retrieves an ancient binary blob from the append-only immutable files.
func (f *freezer) Ancient(kind string, number uint64) ([]byte, error) {
	if table := f.tables[kind]; table != nil {
		return table.Retrieve(number)
	}
	return nil, errUnknownTable
}

// AncientRange retrieves multiple items in sequence, starting from the index 'start'.
// It will return
//   - at most 'max' items,
//   - at least 1 item (even if exceeding the maxByteSize), but will otherwise
//     return as many items as fit into maxByteSize.
func (f *freezer) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if table := f.tables[kind]; table != nil {
		return table.RetrieveItems(start, count, maxBytes)
	}
	return nil, errUnknownTable
}

func (f *freezer) GetOne(bucket string, k []byte) ([]byte, error) {
	number, err := dbutils.DecodeBlockNumber(k[0:8])
	if err != nil {
		return nil, err
	}
	switch bucket {
	case kv.Receipts:
		return f.Ancient(freezerReceiptTable, number)
	case kv2.Entires:
		return f.Ancient(freezerEntiresTable, number)
	case kv.Senders:
		return f.Ancient(freezerSenderTable, number)
	case kv.EthTx:
		return f.Ancient(freezerTransactionsTable, number)
	case kv.HeaderCanonical:
		return f.Ancient(freezerHashTable, number)
	case kv.BlockBody:
		return f.Ancient(freezerBodiesTable, number)
	case kv.Headers:
		return f.Ancient(freezerHeaderTable, number)
	}
	return nil, nil
}

// Ancients returns the length of the frozen items.
func (f *freezer) Ancients() (uint64, error) {
	return atomic.LoadUint64(&f.frozen), nil
}

// Tail returns the number of first stored item in the baseFreezer.
func (f *freezer) Tail() (uint64, error) {
	return atomic.LoadUint64(&f.tail), nil
}

// AncientSize returns the ancient size of the specified category.
func (f *freezer) AncientSize(kind string) (uint64, error) {
	// This needs the write lock to avoid data races on table fields.
	// Speed doesn't matter here, AncientSize is for debugging.
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()

	if table := f.tables[kind]; table != nil {
		return table.size()
	}
	return 0, errUnknownTable
}

// ReadAncients runs the given read operation while ensuring that no writes take place
// on the underlying baseFreezer.
func (f *freezer) ReadAncients(fn func(ethdb.AncientReader) error) (err error) {
	f.writeLock.RLock()
	defer f.writeLock.RUnlock()
	return fn(f)
}

// ModifyAncients runs the given write operation.
func (f *freezer) ModifyAncients(fn func(ethdb.AncientWriteOp) error) (writeSize int64, err error) {
	if f.readonly {
		return 0, errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	// Roll back all tables to the starting position in case of error.
	prevItem := f.frozen
	defer func() {
		if err != nil {
			// The write operation has failed. Go back to the previous item position.
			for name, table := range f.tables {
				err := table.truncateHead(prevItem)
				if err != nil {
					log.Error("Freezer table roll-back failed", "table", name, "index", prevItem, "err", err)
				}
			}
		}
	}()

	f.writeBatch.reset()
	if err := fn(f.writeBatch); err != nil {
		return 0, err
	}
	item, writeSize, err := f.writeBatch.commit()
	if err != nil {
		return 0, err
	}
	atomic.StoreUint64(&f.frozen, item)
	return writeSize, nil
}

func (f *freezer) Rollback() {
	if f.readonly || !f.newRWTx {
		return
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()
	if f.writeBatch.hasData() {
		f.writeBatch.reset()
	}
	f.newRWTx = false
}

func (f *freezer) BeginRW() error {
	if f.readonly {
		return nil
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()
	if f.newRWTx {
		return errRepeatedWriteBusiness
	}
	f.newRWTx = true
	f.writeBatch.reset()
	return nil
}

func (f *freezer) Put(bucket string, k, v []byte) error {
	if f.readonly {
		return errReadOnly
	}
	if !f.newRWTx {
		return errNoWriteBusiness
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()
	number, err := dbutils.DecodeBlockNumber(k)
	if err != nil {
		return err
	}
	switch bucket {
	case kv.Receipts:
		if err := f.writeBatch.AppendBytes(freezerReceiptTable, number, v); err != nil {
			return fmt.Errorf("can't append block %d receipts: %v", number, err)
		}
	case kv.Senders:
		if err := f.writeBatch.AppendBytes(freezerSenderTable, number, v); err != nil {
			return fmt.Errorf("can't append block %d senders: %v", number, err)
		}
	case kv.HeaderCanonical:
		if err := f.writeBatch.AppendBytes(freezerHashTable, number, v); err != nil {
			return fmt.Errorf("can't append block %d senders: %v", number, err)
		}
	case kv2.Entires:
		if err := f.writeBatch.AppendBytes(freezerEntiresTable, number, v); err != nil {
			return fmt.Errorf("can't append block %d entire: %v", number, err)
		}
	case kv.EthTx:
		if err := f.writeBatch.AppendBytes(freezerTransactionsTable, number, v); err != nil {
			return fmt.Errorf("can't append block %d transactions: %v", number, err)
		}
	case kv.BlockBody:
		if err := f.writeBatch.AppendBytes(freezerBodiesTable, number, v); err != nil {
			return fmt.Errorf("can't append block body %d: %v", number, err)
		}
	case kv.Headers:
		if err := f.writeBatch.AppendBytes(freezerHeaderTable, number, v); err != nil {
			return fmt.Errorf("can't append block header %d: %v", number, err)
		}
	}
	return err
}

func (f *freezer) Commit() (writeSize int64, err error) {
	if f.readonly {
		return 0, errReadOnly
	}
	if !f.newRWTx {
		return 0, nil
	}
	f.writeLock.Lock()
	defer func() {
		f.newRWTx = false
	}()
	defer f.writeLock.Unlock()
	if !f.writeBatch.hasData() {
		return 0, nil
	}
	// Roll back all tables to the starting position in case of error.
	prevItem := f.frozen
	defer func() {
		if err != nil {
			// The write operation has failed. Go back to the previous item position.
			for name, table := range f.tables {
				err := table.truncateHead(prevItem)
				if err != nil {
					log.Error("Freezer table roll-back failed", "table", name, "index", prevItem, "err", err)
				}
			}
		}
	}()

	item, writeSize, err := f.writeBatch.commit()
	if err != nil {
		return 0, err
	}
	atomic.StoreUint64(&f.frozen, item)
	return writeSize, nil
}

// TruncateHead discards any recent data above the provided threshold number.
func (f *freezer) TruncateHead(items uint64) error {
	if f.readonly {
		return errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	if atomic.LoadUint64(&f.frozen) <= items {
		return nil
	}
	for _, table := range f.tables {
		if err := table.truncateHead(items); err != nil {
			return err
		}
	}
	atomic.StoreUint64(&f.frozen, items)
	return nil
}

// TruncateTail discards any recent data below the provided threshold number.
func (f *freezer) TruncateTail(tail uint64) error {
	if f.readonly {
		return errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	if atomic.LoadUint64(&f.tail) >= tail {
		return nil
	}
	for _, table := range f.tables {
		if err := table.truncateTail(tail); err != nil {
			return err
		}
	}
	atomic.StoreUint64(&f.tail, tail)
	return nil
}

// Sync flushes all data tables to disk.
func (f *freezer) Sync() error {
	var errs []error
	for _, table := range f.tables {
		if err := table.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// validate checks that every table has the same length.
// Used instead of `repair` in readonly mode.
func (f *freezer) validate() error {
	if len(f.tables) == 0 {
		return nil
	}
	var (
		length uint64
		name   string
	)
	// Hack to get length of any table
	for kind, table := range f.tables {
		length = atomic.LoadUint64(&table.items)
		name = kind
		break
	}
	// Now check every table against that length
	for kind, table := range f.tables {
		items := atomic.LoadUint64(&table.items)
		if length != items {
			return fmt.Errorf("baseFreezer tables %s and %s have differing lengths: %d != %d", kind, name, items, length)
		}
	}
	atomic.StoreUint64(&f.frozen, length)
	return nil
}

// repair truncates all data tables to the same length.
func (f *freezer) repair() error {
	var (
		head = uint64(math.MaxUint64)
		tail = uint64(0)
	)
	for _, table := range f.tables {
		items := atomic.LoadUint64(&table.items)
		if head > items {
			head = items
		}
		hidden := atomic.LoadUint64(&table.itemHidden)
		if hidden > tail {
			tail = hidden
		}
	}
	for _, table := range f.tables {
		if err := table.truncateHead(head); err != nil {
			return err
		}
		if err := table.truncateTail(tail); err != nil {
			return err
		}
	}
	atomic.StoreUint64(&f.frozen, head)
	atomic.StoreUint64(&f.tail, tail)
	return nil
}

// freeze is a background thread that periodically checks the blockchain for any
// import progress and moves ancient data from the fast database into the freezer.
//
// This functionality is deliberately broken off from block importing to avoid
// incurring additional data shuffling delays on block propagation.
func (f *freezer) freeze(db kv.Tx) {
	//nfdb := &nofreezedb{Tx: db}
	//
	//var (
	//	backoff   bool
	//	triggered chan struct{} // Used in tests
	//)
	//for {
	//	select {
	//	case <-f.quit:
	//		log.Info("Freezer shutting down")
	//		return
	//	default:
	//	}
	//	if backoff {
	//		// If we were doing a manual trigger, notify it
	//		if triggered != nil {
	//			triggered <- struct{}{}
	//			triggered = nil
	//		}
	//		select {
	//		case <-time.NewTimer(freezerRecheckInterval).C:
	//			backoff = false
	//		case triggered = <-f.trigger:
	//			backoff = false
	//		case <-f.quit:
	//			return
	//		}
	//	}
	//	// Retrieve the freezing threshold.
	//	hash := rawdb.ReadHeadBlockHash(nfdb)
	//	if hash == (libcommon.Hash{}) {
	//		log.Debug("Current full block hash unavailable") // new chain, empty database
	//		backoff = true
	//		continue
	//	}
	//	number := rawdb.ReadHeaderNumber(nfdb, hash)
	//	threshold := atomic.LoadUint64(&f.threshold)
	//
	//	switch {
	//	case number == nil:
	//		log.Error("Current full block number unavailable", "hash", hash)
	//		backoff = true
	//		continue
	//
	//	case *number < threshold:
	//		log.Debug("Current full block not old enough", "number", *number, "hash", hash, "delay", threshold)
	//		backoff = true
	//		continue
	//
	//	case *number-threshold <= f.frozen:
	//		log.Debug("Ancient blocks frozen already", "number", *number, "hash", hash, "frozen", f.frozen)
	//		backoff = true
	//		continue
	//	}
	//	head := rawdb.ReadHeader(nfdb, hash, *number)
	//	if head == nil {
	//		log.Error("Current full block unavailable", "number", *number, "hash", hash)
	//		backoff = true
	//		continue
	//	}
	//
	//	// Seems we have data ready to be frozen, process in usable batches
	//	var (
	//		start    = time.Now()
	//		first, _ = f.Ancients()
	//		limit    = *number - threshold
	//	)
	//	if limit-first > freezerBatchLimit {
	//		limit = first + freezerBatchLimit
	//	}
	//	ancients, err := f.freezeRange(nfdb, first, limit)
	//	if err != nil {
	//		log.Error("Error in block freeze operation", "err", err)
	//		backoff = true
	//		continue
	//	}
	//
	//	// Batch of blocks have been frozen, flush them before wiping from leveldb
	//	if err := f.Sync(); err != nil {
	//		log.Crit("Failed to flush frozen tables", "err", err)
	//	}
	//
	//	// Wipe out all data from the active database
	//	batch := db.NewBatch()
	//	for i := 0; i < len(ancients); i++ {
	//		// Always keep the genesis block in active database
	//		if first+uint64(i) != 0 {
	//			DeleteBlockWithoutNumber(batch, ancients[i], first+uint64(i))
	//			rawdb.DeleteCanonicalHash(batch, first+uint64(i))
	//		}
	//	}
	//	if err := batch.Write(); err != nil {
	//		log.Crit("Failed to delete frozen canonical blocks", "err", err)
	//	}
	//	batch.Reset()
	//
	//	// Wipe out side chains also and track dangling side chains
	//	var dangling []libcommon.Hash
	//	for number := first; number < f.frozen; number++ {
	//		// Always keep the genesis block in active database
	//		if number != 0 {
	//			dangling = ReadAllHashes(db, number)
	//			for _, hash := range dangling {
	//				log.Trace("Deleting side chain", "number", number, "hash", hash)
	//				rawdb.DeleteBlock(batch, hash, number)
	//			}
	//		}
	//	}
	//	if err := batch.Write(); err != nil {
	//		log.Crit("Failed to delete frozen side blocks", "err", err)
	//	}
	//	batch.Reset()
	//
	//	// Step into the future and delete and dangling side chains
	//	if f.frozen > 0 {
	//		tip := f.frozen
	//		for len(dangling) > 0 {
	//			drop := make(map[libcommon.Hash]struct{})
	//			for _, hash := range dangling {
	//				log.Debug("Dangling parent from baseFreezer", "number", tip-1, "hash", hash)
	//				drop[hash] = struct{}{}
	//			}
	//			children := ReadAllHashes(db, tip)
	//			for i := 0; i < len(children); i++ {
	//				// Dig up the child and ensure it's dangling
	//				child := rawdb.ReadHeader(nfdb, children[i], tip)
	//				if child == nil {
	//					log.Error("Missing dangling header", "number", tip, "hash", children[i])
	//					continue
	//				}
	//				if _, ok := drop[child.ParentHash]; !ok {
	//					children = append(children[:i], children[i+1:]...)
	//					i--
	//					continue
	//				}
	//				// Delete all block data associated with the child
	//				log.Debug("Deleting dangling block", "number", tip, "hash", children[i], "parent", child.ParentHash)
	//				rawdb.DeleteBlock(batch, children[i], tip)
	//			}
	//			dangling = children
	//			tip++
	//		}
	//		if err := batch.Write(); err != nil {
	//			log.Crit("Failed to delete dangling side blocks", "err", err)
	//		}
	//	}
	//
	//	// Log something friendly for the user
	//	context := []interface{}{
	//		"blocks", f.frozen - first, "elapsed", common.PrettyDuration(time.Since(start)), "number", f.frozen - 1,
	//	}
	//	if n := len(ancients); n > 0 {
	//		context = append(context, []interface{}{"hash", ancients[n-1]}...)
	//	}
	//	log.Info("Deep froze chain segment", context...)
	//
	//	// Avoid database thrashing with tiny writes
	//	if f.frozen-first < freezerBatchLimit {
	//		backoff = true
	//	}
	//}
}

func (f *freezer) freezeRange(nfdb kv.Tx, number, limit uint64) (hashes []common2.Hash, err error) {
	hashes = make([]common2.Hash, 0, limit-number)

	_, err = f.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		for ; number <= limit; number++ {
			// Retrieve all the components of the canonical block.
			hash, err := rawdb.ReadCanonicalHash(nfdb, number)
			if err != nil {
				return err
			}
			if hash == (common2.Hash{}) {
				return fmt.Errorf("canonical hash missing, can't freeze block %d", number)
			}
			header := rawdb.ReadHeaderRLP(nfdb, hash, number)
			if len(header) == 0 {
				return fmt.Errorf("block header missing, can't freeze block %d", number)
			}
			body := rawdb.ReadBodyRLP(nfdb, hash, number)
			if len(body) == 0 {
				return fmt.Errorf("block body missing, can't freeze block %d", number)
			}
			//receipts := rawdb.ReadRawReceipts(nfdb, number)
			receipts, _ := nfdb.GetOne(kv.Receipts, hexutility.EncodeTs(number))
			if len(receipts) == 0 {
				return fmt.Errorf("block receipts missing, can't freeze block %d", number)
			}
			// Write to the batch.
			if err := op.AppendRaw(freezerHashTable, number, hash[:]); err != nil {
				return fmt.Errorf("can't write hash to baseFreezer: %v", err)
			}
			if err := op.AppendRaw(freezerHeaderTable, number, header); err != nil {
				return fmt.Errorf("can't write header to baseFreezer: %v", err)
			}
			if err := op.AppendRaw(freezerBodiesTable, number, body); err != nil {
				return fmt.Errorf("can't write body to baseFreezer: %v", err)
			}
			if err := op.AppendRaw(freezerReceiptTable, number, receipts); err != nil {
				return fmt.Errorf("can't write receipts to baseFreezer: %v", err)
			}
			hashes = append(hashes, hash)
		}
		return nil
	})

	return hashes, err
}

// convertLegacyFn takes a raw freezer entry in an older format and
// returns it in the new format.
type convertLegacyFn = func([]byte) ([]byte, error)

// MigrateTable processes the entries in a given table in sequence
// converting them to a new format if they're of an old format.
func (f *freezer) MigrateTable(kind string, convert convertLegacyFn) error {
	if f.readonly {
		return errReadOnly
	}
	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	table, ok := f.tables[kind]
	if !ok {
		return errUnknownTable
	}
	// forEach iterates every entry in the table serially and in order, calling `fn`
	// with the item as argument. If `fn` returns an error the iteration stops
	// and that error will be returned.
	forEach := func(t *freezerTable, offset uint64, fn func(uint64, []byte) error) error {
		var (
			items     = atomic.LoadUint64(&t.items)
			batchSize = uint64(1024)
			maxBytes  = uint64(1024 * 1024)
		)
		for i := offset; i < items; {
			if i+batchSize > items {
				batchSize = items - i
			}
			data, err := t.RetrieveItems(i, batchSize, maxBytes)
			if err != nil {
				return err
			}
			for j, item := range data {
				if err := fn(i+uint64(j), item); err != nil {
					return err
				}
			}
			i += uint64(len(data))
		}
		return nil
	}
	// TODO(s1na): This is a sanity-check since as of now no process does tail-deletion. But the migration
	// process assumes no deletion at tail and needs to be modified to account for that.
	if table.itemOffset > 0 || table.itemHidden > 0 {
		return fmt.Errorf("migration not supported for tail-deleted freezers")
	}
	ancientsPath := filepath.Dir(table.index.Name())
	// Set up new dir for the migrated table, the content of which
	// we'll at the end move over to the ancients dir.
	migrationPath := filepath.Join(ancientsPath, "migration")
	newTable, err := NewFreezerTable(migrationPath, kind, FreezerNoSnappy[kind], false)
	if err != nil {
		return err
	}
	var (
		batch  = newTable.newBatch()
		out    []byte
		start  = time.Now()
		logged = time.Now()
		offset = newTable.items
	)
	if offset > 0 {
		log.Info("found previous migration attempt", "migrated", offset)
	}
	// Iterate through entries and transform them
	if err := forEach(table, offset, func(i uint64, blob []byte) error {
		if i%10000 == 0 && time.Since(logged) > 16*time.Second {
			log.Info("Processing legacy elements", "count", i, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
		out, err = convert(blob)
		if err != nil {
			return err
		}
		if err := batch.AppendRaw(i, out); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := batch.commit(); err != nil {
		return err
	}
	log.Info("Replacing old table files with migrated ones", "elapsed", common.PrettyDuration(time.Since(start)))
	// Release and delete old table files. Note this won't
	// delete the index file.
	table.releaseFilesAfter(0, true)

	if err := newTable.Close(); err != nil {
		return err
	}
	files, err := ioutil.ReadDir(migrationPath)
	if err != nil {
		return err
	}
	// Move migrated files to ancients dir.
	for _, f := range files {
		// This will replace the old index file as a side-effect.
		if err := os.Rename(filepath.Join(migrationPath, f.Name()), filepath.Join(ancientsPath, f.Name())); err != nil {
			return err
		}
	}
	// Delete by now empty dir.
	if err := os.Remove(migrationPath); err != nil {
		return err
	}

	return nil
}