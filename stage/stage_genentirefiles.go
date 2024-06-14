package stage

import (
	"context"
	"errors"
	"fmt"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"os"
	"runtime"
	"runtime/pprof"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/calltracer"
	"starlink-world/erigon-evm/eth/tracers/logger"
	"starlink-world/erigon-evm/ethdb/olddb"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/rlp"
	"starlink-world/erigon-evm/verkle"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon-lib/kv"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/core/types/accounts"
)

type Entire struct {
	Header       *types.Header
	Uncles       []*types.Header
	Transactions [][]byte
	Snap         *state.Snapshot
	Proof        libcommon.Hash
	Senders      []libcommon.Address
}

//type HashCode struct {
//	Hash libcommon.Hash
//	Code []byte
//}

func genEntire(
	quit <-chan struct{},
	contractCodeCache *lru.Cache,
	block *types.Block,
	tx kv.Tx,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
) (*accounts.Account, []byte, error) {
	blockNum := block.NumberU64()
	//stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, initialCycle, cfg.stateStream)
	//if err != nil {
	//	return nil, err
	//}

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
	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	//_, isPoSa := cfg.engine.(consensus.PoSA)
	ibs := state.New(stateReader)
	ibs.BeginWriteSnapshot()
	getTracer := func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error) {
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}
	getHashFn := core.GetHashFn(block.Header(), getHeader)
	//if isPoSa {
	//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, EpochReaderImpl{tx: nil}, ChainReaderImpl{config: cfg.chainConfig, tx: nil, blockReader: cfg.blockReader}, getTracer)
	//} else {
		_, err = core.ExecuteBlockEphemerally(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: nil, blockReader: cfg.blockReader}, getTracer)
	//}
	if err != nil {
		return nil, nil, err
	}
	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	acc, err := stateReader.ReadAccountData(block.Header().Coinbase)
	if err != nil {
		return nil, nil, err
	}
	if blockNum == 5305 {
		batch.Print()
	}
	h := batch.Hash()
	ibs.SetOutHash(h)
	pzgTre := state.NewVerkleTrie(verkle.New(), batch)
	header := block.Header()
	if err := pzgTre.TryUpdate(header.ParentHash[:], header.Root[:]); err != nil {
		return nil, nil, err
	}
	for _, v := range batch.HashMap() {
		if err := pzgTre.TryUpdate(v.K, v.V); err != nil {
			return nil, nil, err
		}
	}
	var senders common.Addresses
	if block.Transactions().Len() > 0 {
		senders, err = rawdb.ReadSenders(tx, block.Hash(), blockNum)
		if err != nil {
			return nil, nil, err
		}
		if len(senders) != block.Transactions().Len() {
			return acc, nil, errors.New("invalid block")
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
	result, err := rlp.EncodeToBytes(entri)
	return acc, result, err
}

func SpawnVerifysEntireStage(fromBlock, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()
	logPrefix := "excuted"
	if err != nil {
		return err
	}
	var stoppedErr error
	f, _ := os.Create("cpu_stage_verifys.prof")
	g, _ := os.Create("mem_stage_verifys.prof")
	// 打开性能分析
	pprof.StartCPUProfile(f)
	defer pprof.WriteHeapProfile(g)
	defer pprof.StopCPUProfile()
	//var GasUsedSec uint64
	limit := runtime.NumCPU()
	c := make(chan struct{}, limit)
	finished := make(chan *info, limit)
	end := make(chan struct{})
	ttl := time.NewTicker(time.Second)
	go func() {
		var GasUsedSec uint64
		var ttx uint64
		var number uint64
		var ttx1 uint64
		var hash libcommon.Hash
		var balance uint256.Int
		var htime uint64
		start := time.Now()
		start0 := time.Now()
		for {
			select {
			case cc := <-finished:
				if cc == nil {
					return
				}
				ttx1 += cc.tx
				ttx += cc.tx
				number = cc.number
				htime = cc.time
				hash = cc.hash
				balance = cc.balance
				GasUsedSec += cc.gas

				select {
				case <-c:
				}
			case <-ttl.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %5d elapsed %v hash %s %26d age %s time %v alloc %s sys %s\n", number, ttx1, GasUsedSec>>20, ttx, common.PrettyDuration(time.Since(start)), hash.TerminalString(), balance.Uint64(),
					common.PrettyAge(time.Unix(int64(htime), 0)), common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
				GasUsedSec = 0
				start = time.Now()
				ttx = 0
			case <-end:
				return

			}
		}
	}()
	if fromBlock > toBlock {
		return nil
	}
	blockNum := uint64(fromBlock)
	var w sync.WaitGroup
	for ; blockNum < toBlock; blockNum++ {
		//fmt.Println("排队", blockNum)
		select {
		case c <- struct{}{}:
		}
		w.Add(1)
		//fmt.Println("进入", blockNum)
		i := blockNum
		go func(number uint64) {
			var gas uint64
			var txc uint64
			var htime uint64
			var hash libcommon.Hash
			var balance uint256.Int
			defer func() {
				select {
				case finished <- &info{gas: gas, number: number, hash: hash, time: htime, balance: balance, tx: txc}:
				}
				w.Done()
			}()
			ttx, err := cfg.db.BeginRo(context.Background())
			if err != nil {
				panic(err)
			}
			defer ttx.Rollback()
			batch := olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)
			defer batch.Rollback()
			stateReader := NewStateHistoryReader(ttx, batch, number)
			ibs := state.New(stateReader)
			//fmt.Println("排队", blockNum)
			vv, err := ttx.GetOne(kv2.Entires, dbutils.EncodeBlockNumber(number))
			if err != nil {
				log.Error("区块不存在", "blockNum", number, err)
				panic(fmt.Sprintf("快照[%d]不存在."))
				panic(err)
			}
			if len(vv) == 0 {
				log.Error("区块不存在", "blockNum", number)
				panic(fmt.Sprintf("快照[%d]不存在."))
			}
			var entire Entire
			if err := rlp.DecodeBytes(vv, &entire); err != nil {
				log.Error("区块不存在", "blockNum", number)
				panic(fmt.Sprintf("快照[%d]不存在."))
			}
			ibs.SetSnapshot(entire.Snap)
			ibs.SetHeight(uint32(number))
			ibs.SetGetOneFun(batch.GetOne)
			txs, err := types.DecodeTransactions(entire.Transactions)
			if err != nil {
				log.Error("区块不存在", "blocknumber", number)
				panic("区块不足")
			}
			block := types.NewBlockFromStorage(hash, entire.Header, txs, entire.Uncles, nil)
			if len(entire.Senders) > 0 {
				block.SendersToTxs(entire.Senders)
			}
			//block.SendersToTxs(block.)
			if block == nil {
				log.Error("区块不存在", "blocknumber", number)
				panic("区块不足")
			}
			htime = block.Header().Time
			//fmt.Println(blockNum, ":", arrays[i].Header.Number.Uint64())
			// state is stored through ethdb batches

			hash = block.Hash()
			acc, err := verifyBlock(block, ttx, batch, cfg, ibs, stateReader, *cfg.vmConfig)
			if err != nil {
				log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", number, "hash", block.Hash().String(), "err", err)
				panic(err)
			}
			balance = acc.Balance
			gas = block.GasUsed()
			txc = uint64(block.Transactions().Len())
			//fmt.Println("区块结束", blockNum)
		}(i)
		//stageProgress = blockNum

		//gas = gas + block.GasUsed()
		//GasUsedSec = GasUsedSec + block.GasUsed()
		//currentStateGas = currentStateGas + block.GasUsed()
		//select {
		//default:
		//case <-logEvery.C:
		//	cumulativeGas, err := rawdb.ReadCumulativeGasUsed(tx, blockNum)
		//	if err != nil {
		//		return err
		//	}
		//	totalGasTmp := new(big.Int).Set(totalGasUsed)
		//	elapsed := time.Since(startTime)
		//	estimateRatio := float64(cumulativeGas.Sub(cumulativeGas, startGasUsed).Uint64()) / float64(totalGasTmp.Sub(totalGasTmp, startGasUsed).Uint64())
		//	var estimatedTime common.PrettyDuration
		//	if estimateRatio != 0 {
		//		estimatedTime = common.PrettyDuration((elapsed.Seconds() / estimateRatio) * float64(time.Second))
		//	}
		//	//logBlock, logTx, logTime = logProgress(logPrefix, logBlock, logTime, blockNum, logTx, lastLogTx, gas, float64(currentStateGas)/float64(gasState), estimatedTime, batch)
		//	gas = 0
		//	tx.CollectMetrics()
		//	syncMetrics[stages.Execution].Set(blockNum)
		//}
		//ll := block.Transactions().Len()
		//ttx += ll
		//blockNum++
		// 每1秒输出 统计信息
		//if time.Since(start) > 1*time.Second {
		//	ttx1 += ttx
		//	var m runtime.MemStats
		//	runtime.ReadMemStats(&m)
		//	fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %5d elapsed %v hash %s %26d age %s time %v alloc %s sys %s\n", block.Header().Number, ttx1, GasUsedSec>>20, ttx, common.PrettyDuration(time.Since(start)), block.Header().Hash().TerminalString(), acc.Balance,
		//		common.PrettyAge(time.Unix(int64(block.Header().Time), 0)), common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
		//	GasUsedSec = 0
		//	ttx = 0
		//	start = time.Now()
		//}
		if blockNum > toBlock {
			fmt.Println("执行完毕.")
			//pprof.StopCPUProfile()
			//pprof.WriteHeapProfile(g)
			break
		}
	}
	w.Wait()
	close(end)
	close(finished)
	close(c)
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", blockNum)
	return stoppedErr
}

func SpawnGenEntireFilesStage(fromBlock, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	if fromBlock > toBlock {
		return nil
	}
	logPrefix := "excuted"
	quit := ctx.Done()
	contractCodeCache, err := lru.New(lruDefaultSize)
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var stoppedErr error
	f, _ := os.Create("cpu_stage_gens.prof")
	g, _ := os.Create("mme_stage_gens.prof")
	// 打开性能分析
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	defer pprof.WriteHeapProfile(g)
	//var GasUsedSec uint64
	limit := runtime.NumCPU()
	c := make(chan struct{}, limit)
	finished := make(chan *info, limit)
	end := make(chan struct{})
	saveEndC := make(chan struct{})
	wait := make(chan struct{}, 1)
	continuego := make(chan struct{}, 1)
	ttl := time.NewTicker(time.Second)
	var w sync.WaitGroup
	var saveWait sync.WaitGroup
	saveWait.Add(1)
	started := fromBlock
	go func() {
		defer saveWait.Done()
		var GasUsedSec uint64
		var ttx uint64
		var number uint64
		var ttx1 uint64
		var hash libcommon.Hash
		var balance uint256.Int
		var htime uint64
		start := time.Now()
		start0 := time.Now()
		snaps := make(map[uint64][]byte, 100000)
		snapSize := 0

		saveF := func() {
			if len(snaps) == 0 {
				return
			}
			wtx, err := cfg.db.BeginRw(context.Background())
			if err != nil {
				panic(err)
			}
			for i := len(snaps); i > 0; i-- {
				if v, ok := snaps[started]; !ok {
					break
				} else {
					wtx.Put(kv2.Entires, dbutils.EncodeBlockNumber(started), v)
					delete(snaps, started)
					snapSize = snapSize - len(v)
					started++
				}
			}
			if err = wtx.Commit(); err != nil {
				wtx.Rollback()
				panic(err)
			}
			if len(snaps) == 0 {
				snaps = nil
				snaps = make(map[uint64][]byte)
				snapSize = 0
			}
		}
		for {
			select {
			case cc := <-finished:
				if cc == nil {
					return
				}
				ttx1 += cc.tx
				ttx += cc.tx
				number = cc.number
				htime = cc.time
				hash = cc.hash
				balance = cc.balance
				GasUsedSec += cc.gas
				snaps[cc.number] = cc.snap
				snapSize += len(cc.snap)
				select {
				case <-c:
				}
			case <-ttl.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %5d elapsed %v hash %s %26d age %s time %v snap %s alloc %s sys %s\n", number, ttx1, GasUsedSec>>20, ttx, common.PrettyDuration(time.Since(start)), hash.TerminalString(), balance.Uint64(),
					common.PrettyAge(time.Unix(int64(htime), 0)), common.PrettyDuration(time.Since(start0)), common.StorageSize(snapSize).String(), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
				GasUsedSec = 0
				start = time.Now()
				ttx = 0
				if snapSize < 256*1024*1024 {
					//if snapSize < 1024 {
					continue
				}
				select {
				case wait <- struct{}{}:
					fmt.Println("卡住来存储区块的快照.")
				}
				saveF()
				select {
				case continuego <- struct{}{}:
					fmt.Println("准备回复,存储完毕.")
				}
				start = time.Now()
			case <-end:
				saveF()
				return

			}
		}
	}()
	blockNum := uint64(fromBlock)
	for ; blockNum <= toBlock; blockNum++ {
		//fmt.Println("排队", blockNum)
		select {
		case <-wait:
			fmt.Println("等待中")
			select {
			case <-continuego:
				fmt.Println("继续运行")
			}
		default:

		}
		select {
		case c <- struct{}{}:
		}
		w.Add(1)
		//fmt.Println("进入", blockNum)
		i := blockNum
		go func(blockNum uint64) {
			var gas uint64
			var txc uint64
			var htime uint64
			var snap []byte
			var hash libcommon.Hash
			var balance uint256.Int
			defer func() {
				info1 := &info{gas: gas, snap: snap, number: blockNum, hash: hash, time: htime, balance: balance, tx: txc}
				select {
				case finished <- info1:
				}
				w.Done()
			}()
			ttx, err := cfg.db.BeginRo(context.Background())
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
			htime = block.Header().Time

			hash = block.Hash()
			var acc *accounts.Account
			acc, snap, err = genEntire(quit, contractCodeCache, block, ttx, cfg, *cfg.vmConfig)
			if err != nil {
				log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
				panic(err)
			}
			balance = acc.Balance
			gas = block.GasUsed()
			txc = uint64(block.Transactions().Len())
		}(i)
	}
	w.Wait()
	close(end)
	saveWait.Wait()
	close(saveEndC)
	close(finished)
	close(c)
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", blockNum)
	return stoppedErr
}
