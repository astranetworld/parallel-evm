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
	"starlink-world/erigon-evm/core/state"
	kv2 "starlink-world/erigon-evm/kv"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/rlp"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/core/types/accounts"
)

func SpawnGenStateSnapFilesStage(fromBlock, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
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
					wtx.Put(kv2.StateSnap, dbutils.EncodeBlockNumber(started), v)
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
			acc, snap, err = genSnapBlock(quit, contractCodeCache, block, ttx, cfg, *cfg.vmConfig)
			if err != nil {
				log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
				panic(err)
			}
			if blockNum == 5305 {
				var sna1p state.Snapshot
				if err := rlp.DecodeBytes(snap, &sna1p); err != nil {
					panic(err)
				}

				//0xfad02c7413b5f870f3c8b3d82c308f7b92250f0550a2a1cf05d83d39a220d4bc
				//fmt.Println(sna1p.OutHash.Hex())
			}

			//val, err := ttx.GetOne(kv.StateSnap, dbutils.EncodeBlockNumber(blockNum))
			//if err != nil {
			//	panic(err)
			//}
			//var s state.Snapshot
			//if err := rlp.DecodeBytes(val, &s); err != nil {
			//	panic(err)
			//}
			//var s2 state.Snapshot
			//if err := rlp.DecodeBytes(snap, &s2); err != nil {
			//	panic(err)
			//}
			//if s.OutHash != s2.OutHash {
			//	fmt.Println(hexutil.Encode(val), hexutil.Encode(snap))
			//	acc, snap, err = genSnapBlock(quit, contractCodeCache, block, ttx, cfg, *cfg.vmConfig, contractHasTEVM)
			//	if err != nil {
			//		log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
			//		panic(err)
			//	}
			//	panic("读取错误.")
			//}
			//if !bytes.Equal(val, snap) {
			//	fmt.Println(hexutil.Encode(val), hexutil.Encode(snap))
			//	acc, snap, err = genSnapBlock(quit, contractCodeCache, block, ttx, cfg, *cfg.vmConfig, contractHasTEVM)
			//	if err != nil {
			//		log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
			//		panic(err)
			//	}
			//	panic("读取错误.")
			//}
			balance = acc.Balance
			gas = block.GasUsed()
			txc = uint64(block.Transactions().Len())
			//fmt.Println("区块结束", blockNum)
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
