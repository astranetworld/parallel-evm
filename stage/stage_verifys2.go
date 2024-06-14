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
	"starlink-world/erigon-evm/core/types"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/log"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/ethdb/olddb"
)

type blockinfo2 struct {
	block *types.Block
	snap  []byte
}

func SpawnVerifysBlock2Stage(fromBlock, toBlock uint64, useSnap bool, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()
	logPrefix := "excuted"
	if fromBlock > toBlock {
		return nil
	}
	if err != nil {
		return err
	}
	var stoppedErr error
	f, _ := os.Create("cpu_stage_verifys2.prof")
	g, _ := os.Create("mem_stage_verifys2.prof")
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
	//threads := uint64(runtime.NumCPU())
	threads := uint64(runtime.NumCPU() / 2)
	fmt.Println("threads", threads)
	times := uint64(10000) //数组扩容,threads*times表示准备的数组的长度,
	cacheSize := threads * times
	arrays := make([]blockinfo2, cacheSize)
	arrayreadys := make([]bool, cacheSize)
	var a sync.WaitGroup
	fmt.Println("准备队列")
	readf := func(ttx kv.Tx, number uint64, index uint64) {
		block, err := readBlock(ttx, ctx, cfg, number, logPrefix)
		if err != nil {
			panic(err)
		}
		if block == nil {
			log.Error("区块不存在", "blocknumber", number)
			panic("区块不足")
		}
		b := blockinfo2{block: block}
		if useSnap {
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
			b.snap = vv
		}
		arrays[index] = b
		arrayreadys[index] = true
	}
	poschan := make([]chan uint64, threads)
	for i := uint64(0); i < threads; i++ {
		poschan[i] = make(chan uint64, times)
	}
	startBlockNumber := fromBlock
	endC := make(chan struct{})
	for i := uint64(0); i < threads; i++ {
		ii := i
		a.Add(1)
		go func(pos uint64) {
			index := pos
			ttx, err := cfg.db.BeginRo(context.Background())
			if err != nil {
				panic(err)
			}
			defer ttx.Rollback()
			for ; pos < cacheSize; pos = pos + threads {
				readf(ttx, pos+startBlockNumber, pos)
			}
			a.Done()
			for {
				select {
				case <-endC:
					return
				case p := <-poschan[index]:
					newIndex := (p - startBlockNumber) % cacheSize
					if p+cacheSize > toBlock {
						continue
					}
					readf(ttx, p+cacheSize, newIndex)
				}
			}
		}(ii)
	}
	a.Wait()
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
	blockNum := uint64(fromBlock)
	var w sync.WaitGroup
	for ; blockNum < toBlock; blockNum++ {
		select {
		case c <- struct{}{}:
		}
		w.Add(1)
		//fmt.Println("进入", blockNum)
		i := blockNum
		go func(number uint64) {
			index := (number - startBlockNumber) % (cacheSize)
			for !arrayreadys[index] || arrays[index].block.NumberU64() != number {
				fmt.Println(fmt.Sprintf("区块%d,数组索引%d的数据没有准备好,等待1秒", number, index))
				time.Sleep(time.Second * 1)
			}
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
			param := arrays[index]
			if number != param.block.NumberU64() {
				fmt.Println("panic index", index, "目标number", number, param.block.NumberU64())
				panic("ee")
			}
			block := param.block
			ttx, err := cfg.db.BeginRo(context.Background())
			if err != nil {
				panic(err)
			}
			defer ttx.Rollback()
			batch := olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)
			defer batch.Rollback()
			stateReader := NewStateHistoryReader(ttx, batch, number)
			ibs := state.New(stateReader)
			ibs.SetHeight(uint32(number))
			if useSnap {
				if number == 50222 {
					fmt.Println()
				}
				ibs.PrepareReadableSnapshot(param.snap)
				ibs.SetGetOneFun(batch.GetOne)
			}
			htime = block.Header().Time

			hash = block.Hash()
			acc, err := verifyBlock(block, ttx, batch, cfg, ibs, stateReader, *cfg.vmConfig)
			if err != nil {
				log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", number, "block2", block.NumberU64(), "hash", block.Hash().String(), "OutHash", ibs.Snap().OutHash, "len", len(ibs.Snap().Items), "OutHash", ibs.Snap().OutHash, "err", err)
				panic(err)
			}
			balance = acc.Balance
			gas = block.GasUsed()
			txc = uint64(block.Transactions().Len())
			arrayreadys[index] = false
			select {
			case poschan[(number-startBlockNumber)%threads] <- number:
			}
		}(i)
		if blockNum > toBlock {
			fmt.Println("执行完毕.")
			break
		}
	}
	w.Wait()
	close(end)
	close(finished)
	close(c)
	close(endC)
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", blockNum)
	return stoppedErr
}
