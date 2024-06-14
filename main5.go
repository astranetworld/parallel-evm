package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"path"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/ethdb/cbor"
	kv2 "starlink-world/erigon-evm/kv/mdbx"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/rlp"
)

//将mdbx里边的数据写入到文件中,参考eth的文件存储方式,注意,仅仅存储Receipts,Senders,BlockBody,EthTx(其实为区块的body),Headers,HeaderCanonical

var (
	datadir                        = "/Volumes/1T/erigon"
	fromBlockNumber                = uint64(0)
	stopBlockNumber                = uint64(2000000)
	chaindata                      = path.Join(datadir, "chaindata")
	databaseVerbosity              = int(2)
	referenceChaindata             string
	block, pruneTo, unwind         uint64
	unwindEvery                    uint64
	freezer                        = "/Volumes/1T/chaindata"
	batchSizeStr                   = "512M"
	reset                          bool
	bucket                         string
	toChaindata                    string
	migration                      string
	integrityFast                  = true
	integritySlow                  bool
	file                           string
	HeimdallURL                    string
	txtrace                        bool // Whether to trace the execution (should only be used together eith `block`)
	pruneFlag                      = "hrtc"
	pruneH, pruneR, pruneT, pruneC uint64
	pruneHBefore, pruneRBefore     uint64
	pruneTBefore, pruneCBefore     uint64
	experiments                    []string
	chain                          string // Which chain to use (mainnet, ropsten, rinkeby, goerli, etc.)
	syncmodeStr                    string
	emptySliceByte, _              = rlp.EncodeToBytes([]interface{}{})
	receiptSliceByte, _            = rlp.EncodeToBytes([]interface{}{})
)

func init() {
	receipts := make(types.Receipts, 0)
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if err := cbor.Marshal(buf, receipts); err != nil {
		log.Error("receipt unmarshal failed", "err", err)
	}
	receiptSliceByte = buf.Bytes()
}

func openKV(label kv.Label, logger log.Logger, path string, exclusive bool) kv.RwDB {
	opts := kv2.NewMDBX(logger).Freezer(freezer).NoBaseFreezerReadable(true).NoSnapFreezerReadable(true).Path(path).Label(label)
	if exclusive {
		opts = opts.Exclusive()
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts.MustOpen()
}

func openDB(path string, logger log.Logger, applyMigrations bool) kv.RwDB {
	label := kv.ChainDB
	db := openKV(label, logger, path, false)
	return db
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func rewriteBlock(db kv.RwTx, number uint64) error {
	hash, err := rawdb.ReadCanonicalHash(db, number)
	if err != nil {
		return fmt.Errorf("requested non-canonical hash %x.", hash)
	}
	headerData := rawdb.ReadHeaderRLP(db, hash, number)
	if len(headerData) == 0 {
		return fmt.Errorf("requested header hash %x.", hash)
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(headerData), header); err != nil {
		return fmt.Errorf("requested error header hash %x.", hash)
	}
	storageBodyRLP := rawdb.ReadStorageBodyRLP(db, hash, number)
	if len(storageBodyRLP) == 0 {
		return fmt.Errorf("requested storage body rlp hash %x.", hash)
	}
	bodyForStorage := new(types.BodyForStorage)
	if err := rlp.DecodeBytes(storageBodyRLP, bodyForStorage); err != nil {
		return fmt.Errorf("requested error storage body rlp hash %x.", hash)
	}
	body := new(types.Body)
	body.Uncles = bodyForStorage.Uncles

	if bodyForStorage.TxAmount < 2 {
		panic(fmt.Sprintf("block body hash too few txs amount: %d, %d", number, bodyForStorage.TxAmount))
	}
	baseTxId, txAmount := bodyForStorage.BaseTxId+1, bodyForStorage.TxAmount-2
	transactions, err := rawdb.CanonicalTransactions(db, baseTxId, txAmount)
	if err != nil {
		log.Error("failed ReadTransactionByHash", "hash", hash, "block", number, "err", err)
		return nil
	}
	receiptsData, err := db.GetOne(kv.Receipts, dbutils.HeaderKey(number, hash))
	if err != nil {
		log.Error("ReadHeaderRLP failed", "err", err)
	}
	if len(receiptsData) == 0 {
		receiptsData = receiptSliceByte
	}
	numberBytes := dbutils.EncodeBlockNumber(number)
	if len(transactions) == 0 {
		if err := db.Put(kv.EthTx, numberBytes, emptySliceByte); err != nil {
			return err
		}
		if err := db.Put(kv.Receipts, numberBytes, receiptsData); err != nil {
			return err
		}
		if err := db.Put(kv.Senders, numberBytes, emptySliceByte); err != nil {
			return err
		}
		if err := db.Put(kv.BlockBody, numberBytes, storageBodyRLP); err != nil {
			return err
		}
		if err := db.Put(kv.Headers, numberBytes, headerData); err != nil {
			return err
		}
		if err := db.Put(kv.HeaderCanonical, numberBytes, hash[:]); err != nil {
			return err
		}
		return db.Commit()
	}
	senders, err := db.GetOne(kv.Senders, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return fmt.Errorf("readSenders failed: %w", err)
	}
	if len(senders)/length.Addr != len(transactions) {
		return fmt.Errorf("requested senders non-canonical hash %x.", hash)
	}
	if err := db.Put(kv.Receipts, numberBytes, receiptsData); err != nil {
		return err
	}
	if err := db.Put(kv.Senders, numberBytes, senders); err != nil {
		return err
	}
	if err := db.Put(kv.BlockBody, numberBytes, storageBodyRLP); err != nil {
		return err
	}
	if err := db.Put(kv.EthTx, numberBytes, rlp.MustEncodeToBytes(transactions)); err != nil {
		return err
	}
	if err := db.Put(kv.Headers, numberBytes, headerData); err != nil {
		return err
	}
	if err := db.Put(kv.HeaderCanonical, numberBytes, hash[:]); err != nil {
		return err
	}
	return db.Commit()
}

func main() {
	logger := log.New()
	db := openDB(chaindata, logger, true)
	defer db.Close()
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	if fromBlockNumber > stopBlockNumber {
		return
	}

	logPrefix := "excuted"
	//f, _ := os.Create("cpu_main5.prof")
	//g, _ := os.Create("mem_main5.prof")
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	//defer pprof.WriteHeapProfile(g)
	switch db.(type) {
	case *kv2.MdbxKV:
		v := db.(*kv2.MdbxKV)
		old, err := v.Ancients()
		if err != nil {
			panic(err)
			return
		}
		i := fromBlockNumber
		if old >= i && old > 0 {
			i = old
		}
		for ; i <= stopBlockNumber; i++ {
			wtx, err := db.BeginRw(context.Background())
			if err != nil {
				wtx.Rollback()
				panic(err)
			}
			if err := rewriteBlock(wtx, i); err != nil {
				wtx.Rollback()
				panic(err)
				return
			}
		}
		log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", fromBlockNumber)
	default:
		return

	}
	return
}
