package stage

import (
	"context"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"io/ioutil"
	"path"
	"sort"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/calltracer"
	"starlink-world/erigon-evm/eth/tracers/logger"
	"starlink-world/erigon-evm/ethdb/olddb"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/rlp"
	"starlink-world/erigon-evm/verkle"
)

type EntireCode struct {
	Entire *Entire
	Codes  common.Hashes
	//Codes  []*HashCode
}

func GenEntireCode(blockNum uint64, dir string, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	logPrefix := "excuted"
	quit := ctx.Done()
	contractCodeCache, err := lru.New(5)
	if err != nil {
		return err
	}
	//f, _ := os.Create("cpu_stage_gens.prof")
	//g, _ := os.Create("mme_stage_gens.prof")
	//// 打开性能分析
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	//defer pprof.WriteHeapProfile(g)
	//var GasUsedSec uint64
	ttx, err := cfg.db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer ttx.Rollback()
	block, err := readBlock(ttx, ctx, cfg, blockNum, logPrefix)
	if err != nil {
		panic(err)
	}
	if block == nil {
		log.Error("区块不存在", "blocknumber", blockNum)
		panic("区块不足")
	}

	entireCode, err := genEntireCode(quit, contractCodeCache, block, ttx, cfg, *cfg.vmConfig)
	if err != nil {
		log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
		panic(err)
	}
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", blockNum)
	rcode, _ := rlp.EncodeToBytes(entireCode)
	if err := ioutil.WriteFile(path.Join(dir, fmt.Sprintf("%d.block", blockNum)), rcode, 0777); err != nil {
		panic(err)
	}
	return nil
}

func genEntireCode(
	quit <-chan struct{},
	contractCodeCache *lru.Cache,
	block *types.Block,
	tx kv.RwTx,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
) (*EntireCode, error) {
	blockNum := block.NumberU64()
	batch := olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)
	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets
	var err error
	stateReader = NewStateHistoryReader(tx, batch, blockNum)
	stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(nil)
	// where the magic happens
	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}
	getTracer := func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error) {
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}
	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	//_, isPoSa := cfg.engine.(consensus.PoSA)
	ibs := state.New(stateReader)
	ibs.BeginWriteSnapshot()
	ibs.BeginWriteCodes()
	getHashFn := core.GetHashFn(block.Header(), getHeader)
	//if isPoSa {
	//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, EpochReaderImpl{tx: tx}, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	//} else {
		_, err = core.ExecuteBlockEphemerally(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	//}
	if err != nil {
		return nil, err
	}
	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	h := batch.Hash()
	ibs.SetOutHash(h)
	pzgTre := state.NewVerkleTrie(verkle.New(), batch)
	header := block.Header()
	if err := pzgTre.TryUpdate(header.ParentHash[:], header.Root[:]); err != nil {
		return nil, err
	}
	for _, v := range batch.HashMap() {
		if err := pzgTre.TryUpdate(v.K, v.V); err != nil {
			return nil, err
		}
	}
	var senders common.Addresses
	if block.Transactions().Len() > 0 {
		senders, err = rawdb.ReadSenders(tx, block.Hash(), blockNum)
		if err != nil {
			return nil, err
		}
		if len(senders) != block.Transactions().Len() {
			return nil, errors.New("invalid block")
		}
	}
	txs := make([][]byte, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		var err error
		txs[i], err = rlp.EncodeToBytes(tx)
		if err != nil {
			panic(err)
		}
	}
	entri := &Entire{Header: block.Header(), Uncles: block.Uncles(), Transactions: txs, Senders: senders, Snap: ibs.Snap(), Proof: pzgTre.Hash()}
	cs := ibs.CodeHashes()
	hs := make(common.Hashes, 0, len(cs))
	for k := range cs {
		hs = append(hs, k)
	}
	sort.Sort(hs)
	return &EntireCode{Codes: hs, Entire: entri}, err
	//codes := make([]*HashCode, len(cs))
	//for k, v := range hs {
	//	codes[k] = &HashCode{Hash: v, Code: cs[v]}
	//}
	//return &EntireCode{Codes: codes, Entire: entri}, err
}
