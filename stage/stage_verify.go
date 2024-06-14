package stage

import (
	"context"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"runtime"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/types/accounts"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/calltracer"
	"starlink-world/erigon-evm/eth/tracers/logger"
	"starlink-world/erigon-evm/ethdb"
	"starlink-world/erigon-evm/ethdb/olddb"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/log"
	"time"
)

func verifyBlock(
	block *types.Block,
	tx kv.Tx,
	batch ethdb.Database,
	cfg ExecuteBlockCfg,
	ibs *state.IntraBlockState,
	stateReader state.StateReader,
	vmConfig vm.Config, // emit copy, because will modify it
) (*accounts.Account, error) {
	var stateWriter state.WriterWithChangeSets
	var err error
	stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(nil)
	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}
	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	//_, isPoSa := cfg.engine.(consensus.PoSA)
	getTracer := func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error) {
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}
	getHashFn := core.GetHashFn(block.Header(), getHeader)
	//if isPoSa {
	//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, EpochReaderImpl{tx: nil}, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	//} else {
		_, err = core.ExecuteBlockEphemerally(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter,ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	//}
	if err != nil {
		return nil, err
	}
	acc, err := stateReader.ReadAccountData(block.Header().Coinbase)
	if err != nil {
		return nil, err
	}
	return acc, err
}

func SpawnVerifyBlocksStage(fromBlock, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	if fromBlock > toBlock {
		return nil
	}
	quit := ctx.Done()

	logPrefix := "excuted"
	if err != nil {
		return err
	}
	var stoppedErr error
	ttx, err := cfg.db.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer func() {
		fmt.Println("Rollback begin")
		ttx.Rollback()
		fmt.Println("Rollback end")
	}()
	GasUsedSec := uint64(0)
	ttx1 := 0
	ttx0 := 0
	start := time.Now()
	start0 := time.Now()

	//m := make(map[uint64][]byte, toBlock-fromBlock+1)
	fmt.Println("开始准备数据")
	//for i := fromBlock; i <= toBlock; i++ {
	//	vv, err := ttx.GetOne(kv.StateSnap, dbutils.EncodeBlockNumber(i))
	//	if err != nil {
	//		panic(err)
	//	}
	//	if len(vv) == 0 {
	//		continue
	//	}
	//	m[i] = vv
	//}
	//f, _ := os.Create("cpu_main4file.prof")
	//g, _ := os.Create("mem_main4file.prof")
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	//defer pprof.WriteHeapProfile(g)
	batch := olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)
	defer batch.Rollback()

	stateReader := NewStateHistoryReader(ttx, batch, fromBlock)
	for ; fromBlock <= toBlock; fromBlock++ {
		if fromBlock == 5305 {
			fromBlock = 5305 - 1 + 1
		}
		stateReader.SetBlockNumber(fromBlock)
		ibs := state.New(stateReader)
		//fmt.Println("排队", fromBlock)
		vv, err := ttx.GetOne(kv2.StateSnap, dbutils.EncodeBlockNumber(fromBlock))
		if err != nil {
			log.Error("区块不存在", "fromBlockber", fromBlock, err)
			panic(fmt.Sprintf("快照[%d]不存在."))
			panic(err)
		}
		if len(vv) == 0 {
			log.Error("区块不存在", "fromBlockber", fromBlock)
			panic(fmt.Sprintf("快照[%d]不存在."))
			continue
		}
		block, err := readBlock(ttx, ctx, cfg, fromBlock, logPrefix)
		if err != nil {
			panic(err)
		}
		if block == nil {
			log.Error("区块不存在", "fromBlockber", fromBlock)
			panic("区块不足")
		}
		ibs.PrepareReadableSnapshot(vv)
		ibs.SetHeight(uint32(fromBlock))
		ibs.SetGetOneFun(batch.GetOne)
		htime := block.Header().Time

		batch.StartRecord()
		acc, err := verifyBlock(block, ttx, batch, cfg, ibs, stateReader, *cfg.vmConfig)
		if err != nil {
			log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", fromBlock, "hash", block.Hash().String(), "err", err)
			panic(err)
		}
		GasUsedSec = GasUsedSec + block.GasUsed()
		ttx0 += block.Transactions().Len()
		ttx1 += block.Transactions().Len()
		if time.Now().Sub(start) > time.Second {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %5d elapsed %v hash %s %26d age %s time %v alloc %s sys %s\n", fromBlock, ttx1, GasUsedSec>>20, ttx0, common.PrettyDuration(time.Since(start)), block.Hash().TerminalString(), acc.Balance.Uint64(),
				common.PrettyAge(time.Unix(int64(htime), 0)), common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
			GasUsedSec = 0
			start = time.Now()
			ttx0 = 0
		}
	}
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", fromBlock)
	return stoppedErr
}
