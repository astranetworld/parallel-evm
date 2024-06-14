package stage

import (
	"context"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/calltracer"
	"starlink-world/erigon-evm/eth/tracers/logger"
	"starlink-world/erigon-evm/ethdb/olddb"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/log"
	"time"
)

func SpawnGenBlockStage(blockNumber uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()
	logPrefix := "excuted"
	if err != nil {
		return err
	}

	// changes are stored through memory buffer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var stoppedErr error
	//f, _ := os.Create("cpu_erigon.prof")
	//g, _ := os.Create("mem_erigon.prof")
	//// 打开性能分析
	//pprof.StartCPUProfile(f)
	//var GasUsedSec uint64
	ttx, err := cfg.db.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer ttx.Rollback()
	block, err := readBlock(ttx, ctx, cfg, blockNumber, logPrefix)
	if err != nil {
		panic(err)
	}
	if block == nil {
		log.Error("区块不存在", "blockNumber", blockNumber)
		panic("区块不足")
	}

	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), ttx, hash, number)
		return h
	}
	val, err := ttx.GetOne(kv2.StateSnap, dbutils.EncodeBlockNumber(blockNumber))
	if err != nil {
		return err
	}
	//_, isPoSa := cfg.engine.(consensus.PoSA)
	var oldHash libcommon.Hash
	var batchbatch *olddb.Mapmutation
	if len(val) == 0 {
		fmt.Println("区块号的state快照为空,先准备快照", blockNumber)
		batchbatch = olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)
		defer batchbatch.Rollback()
		stateReader := NewStateHistoryReader(ttx, batchbatch, blockNumber)
		stateWriter := state.NewPlainStateWriterNoHistory(batchbatch).SetAccumulator(nil)
		callTracer := calltracer.NewCallTracer()
		cfg.vmConfig.Debug = false
		cfg.vmConfig.Tracer = callTracer
		ibs := state.New(stateReader)
		ibs.BeginWriteSnapshot()
		getTracer := func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error) {
			return logger.NewStructLogger(&logger.LogConfig{}), nil
		}
		getHashFn := core.GetHashFn(block.Header(), getHeader)
		//if isPoSa {
		//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, EpochReaderImpl{tx: nil}, ChainReaderImpl{config: cfg.chainConfig, tx: ttx, blockReader: cfg.blockReader}, getTracer)
		//} else {
			_, err = core.ExecuteBlockEphemerally(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: ttx, blockReader: cfg.blockReader}, getTracer)
		//}
		if err != nil {
			log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNumber, "hash", block.Hash().String(), "err", err)
			return err
		}
		if cfg.changeSetHook != nil {
			//if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			//	cfg.changeSetHook(blockNumber, hasChangeSet.ChangeSetWriter())
			//}
		}
		log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", blockNumber)
		oldHash = batchbatch.Hash()
		snap := ibs.WrittenSnapshot(oldHash)
		val = snap
		wtx, err := cfg.db.BeginRw(context.Background())
		if err != nil {
			panic(err)
		}
		if err := wtx.Put(kv2.StateSnap, dbutils.EncodeBlockNumber(blockNumber), snap); err != nil {
			wtx.Rollback()
			return err
		}
		if err = wtx.Commit(); err != nil {
			wtx.Rollback()
			return err
		}
		//ttx, err := cfg.db.BeginRo(context.Background())
		//if err != nil {
		//	panic(err)
		//}
		//val, err = ttx.GetOne(kv.StateSnap, dbutils.EncodeBlockNumber(blockNumber))
		//if err != nil {
		//	return err
		//}
		//if !bytes.Equal(val, snap) {
		//	panic("读取错误.")
		//}
	}
	//batch2 := olddb.NewHashBatch(nil, quit, cfg.tmpdir, whitelistedTables, contractCodeCache)
	//defer batch2.Rollback()
	//stateReader2 := NewStateHistoryReader(ttx, batch2, blockNumber)
	//stateWriter2 := state.NewPlainStateWriterNoHistory(batch2).SetAccumulator(nil)
	//callTracer2 := calltracer.NewCallTracer(contractHasTEVM)
	//
	//ibs2 := state.New(stateReader2)
	//ibs2.PrepareReadableSnapshot(val)
	//cfg.vmConfig.Debug = false
	//cfg.vmConfig.Tracer = callTracer2
	//if isPoSa {
	//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, cfg.vmConfig, getHeader, cfg.engine, block, stateReader2, stateWriter2, epochReader{tx: nil}, chainReader{config: cfg.chainConfig, tx: ttx, blockReader: cfg.blockReader}, contractHasTEVM)
	//} else {
	//	_, _, err = core.ExecuteBlockEphemerally(cfg.chainConfig, cfg.vmConfig, getHeader, cfg.engine, block, stateReader2, stateWriter2, epochReader{tx: nil}, chainReader{config: cfg.chainConfig, tx: ttx, blockReader: cfg.blockReader}, ibs2, contractHasTEVM)
	//}
	//if err != nil {
	//	log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNumber, "hash", block.Hash().String(), "err", err)
	//	return err
	//}
	//h := batch2.Hash()
	//if !ibs2.CheckReadableSnap(h) {
	//	batchbatch.Print()
	//	fmt.Println("换行")
	//	batch2.Print()
	//	fmt.Println("验证失败", h.Hex(), oldHash.Hex())
	//}
	return stoppedErr
}
