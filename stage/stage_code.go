package stage

import (
	"bufio"
	"context"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/calltracer"
	"starlink-world/erigon-evm/eth/tracers/logger"
	"starlink-world/erigon-evm/ethdb"
	"starlink-world/erigon-evm/ethdb/olddb"
	"starlink-world/erigon-evm/log"
	"sync"
	"time"
)

func SpawnCode(toBlock uint64, snap string, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()
	logPrefix := "excuted"

	var stoppedErr error
	f, _ := os.Create("cpu_erigon.prof")
	g, _ := os.Create("mem_erigon.prof")
	// 打开性能分析
	pprof.StartCPUProfile(f)
	defer pprof.WriteHeapProfile(g)
	defer pprof.StopCPUProfile()
	//var GasUsedSec uint64
	limit := runtime.NumCPU()
	c := make(chan struct{}, limit*10)
	finished := make(chan map[libcommon.Hash][]byte, limit)

	ttl := time.NewTicker(time.Second)
	a := sync.WaitGroup{}
	w := sync.WaitGroup{}
	a.Add(1)
	os.Remove(path.Join(snap, "code.dat"))
	os.Remove(path.Join(snap, "code.ind"))
	go func() {
		defer a.Done()
		start := time.Now()
		start0 := time.Now()
		f1, err := os.OpenFile(path.Join(snap, "code.dat"), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		defer f1.Close()
		f2, err := os.OpenFile(path.Join(snap, "code.ind"), os.O_RDWR|os.O_CREATE, 0644)
		defer f2.Close()
		if err != nil {
			panic(err)
		}
		io1 := bufio.NewWriter(f1)
		io2 := bufio.NewWriter(f2)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("panic", r)
			}
			if err := io1.Flush(); err != nil {
				panic(err)
			}
			if err := io2.Flush(); err != nil {
				panic(err)
			}
			fmt.Println("写入完毕!")
		}()
		exist := make(map[libcommon.Hash]struct{})
		l := uint64(0)
		size := uint64(0)
		for {
			select {
			case cc := <-finished:
				if cc == nil {
					return
				}
				size++
				for k, v := range cc {
					if _, ok := exist[k]; ok {
						continue
					}
					if k == libcommon.HexToHash("0xc273fb55b792b536fcb1705b92c1174f1c42184924db6991c40acb54baa45560") {
						fmt.Println("LLL:", k.Hex())
					}
					io1.Write(v)                            //写的是合约代码
					io2.Write(k.Bytes())                    //是codehash
					io2.Write(dbutils.EncodeBlockNumber(l)) //开始的长度
					l = l + uint64(len(v))
					io2.Write(dbutils.EncodeBlockNumber(l)) //结束的长度
					exist[k] = struct{}{}
				}
			case <-ttl.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				fmt.Printf("blocks has code %6d t %v  time %v alloc %s sys %s\n", size, common.PrettyDuration(time.Since(start)), common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
				start = time.Now()
			}
		}
	}()

	blockNum := uint64(1)
	execF := func(num uint64, w *sync.WaitGroup, c <-chan struct{}) {
		defer w.Done()
		var code map[libcommon.Hash][]byte
		defer func() {
			<-c
		}()

		var batch ethdb.DbWithPendingMutations
		ttx, err := cfg.db.BeginRw(context.Background())
		if err != nil {
			panic(err)
		}
		defer ttx.Rollback()
		block, err := readBlock(ttx, ctx, cfg, num, logPrefix)
		if err != nil {
			panic(err)
		}
		if block == nil {
			log.Error("区块不存在", "blocknumber", num)
			panic("区块不足")
		}

		batch = olddb.NewHashBatch(ttx, quit, cfg.dirs.Tmp)

		defer batch.Rollback()
		var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets
		stateReader = NewStateHistoryReader(ttx, batch, num)
		stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(nil)
		// where the magic happens
		getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
			h, _ := cfg.blockReader.Header(context.Background(), ttx, hash, number)
			return h
		}

		callTracer := calltracer.NewCallTracer()
		cfg.vmConfig.Debug = false
		cfg.vmConfig.Tracer = callTracer

		//_, isPoSa := cfg.engine.(consensus.PoSA)
		ibs := state.New(stateReader)
		ibs.BeginWriteSnapshot()
		ibs.BeginWriteCodes()
		getTracer := func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error) {
			return logger.NewStructLogger(&logger.LogConfig{}), nil
		}
		getHashFn := core.GetHashFn(block.Header(), getHeader)
		//if isPoSa {
		//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, EpochReaderImpl{tx: ttx}, ChainReaderImpl{config: cfg.chainConfig, tx: ttx, blockReader: cfg.blockReader}, getTracer)
		//} else {
			_, err = core.ExecuteBlockEphemerally(cfg.chainConfig, cfg.vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: ttx, blockReader: cfg.blockReader}, getTracer)
		//}
		if err != nil {
			return
		}
		if cfg.changeSetHook != nil {
			if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
				cfg.changeSetHook(num, hasChangeSet.ChangeSetWriter())
			}
		}

		code = ibs.CodeHashes()
		if len(code) == 0 {
			return
		}
		finished <- code
	}
	for ; blockNum <= toBlock; blockNum++ {
		//fmt.Println("排队", blockNum)
		c <- struct{}{}
		w.Add(1)
		//fmt.Println("进入", blockNum)

		i := blockNum
		go execF(i, &w, c)
	}
	fmt.Println("Wait")
	w.Wait()
	fmt.Println("Wait finished")
	close(finished)
	close(c)
	a.Wait()

	//close(end)
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", blockNum)
	fmt.Println("jieshu !")
	return stoppedErr
}
