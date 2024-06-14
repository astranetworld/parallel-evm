package stage

import (
	"context"
	"fmt"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"os"
	"runtime"
	"runtime/pprof"
	"starlink-world/erigon-evm/common/dbutils"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/log"
	"sync"
	"time"

	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/ethdb/olddb"
)

func SpawnVerifysBlocksStage(fromBlock, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
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
			vv, err := ttx.GetOne(kv2.StateSnap, dbutils.EncodeBlockNumber(number))
			if err != nil {
				log.Error("区块不存在", "blockNum", number, err)
				panic(fmt.Sprintf("快照[%d]不存在."))
				panic(err)
			}
			if len(vv) == 0 {
				log.Error("区块不存在", "blockNum", number)
				panic(fmt.Sprintf("快照[%d]不存在."))
			}
			ibs.PrepareReadableSnapshot(vv)
			ibs.SetHeight(uint32(number))
			ibs.SetGetOneFun(batch.GetOne)
			block, err := readBlock(ttx, ctx, cfg, number, logPrefix)
			if err != nil {
				panic(err)
			}
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
