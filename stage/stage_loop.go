package stage

import (
	"bytes"
	"context"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"runtime"
	"starlink-world/erigon-evm/common"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/rlp"
	"sync"
	"time"
	"unsafe"
)

type progress struct {
	index  int
	number uint64
}
type readF func(db kv.Tx, number uint64) (interface{}, bool)

func readHeader(db kv.Tx, number uint64) (interface{}, bool) {
	hash, err := rawdb.ReadCanonicalHash(db, number)
	if err != nil {
		fmt.Println(fmt.Sprintf("requested non-canonical hash %x.", hash))
		return nil, false
	}
	headerData := rawdb.ReadHeaderRLP(db, hash, number)
	if len(headerData) == 0 {
		return nil, false
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(headerData), header); err != nil {
		return nil, false
	}
	return header, true
}

func headerLoop(wait *sync.WaitGroup, ctx context.Context, rF readF, cache []interface{}, fromBlock, toBlock uint64, endC <-chan struct{}, cfg ExecuteBlockCfg, finishedC chan *progress, planChs []chan uint64, endChs []chan uint64) {
	defer wait.Done()
	if len(cache) == 0 {
		return
	}
	max := uint64(len(cache))
	quit := ctx.Done()
	timer := time.NewTicker(time.Second * 2)
	notice := time.NewTicker(time.Second)
	db, err := cfg.db.BeginRo(ctx)
	if err != nil {
		panic(err)
	}
	posF := func(n uint64) uint64 {
		return (n - fromBlock) % max
	}
	number := fromBlock
	thread := uint64(runtime.NumCPU())
	finished := make(map[int]uint64, len(planChs))
	noticed := make(map[int]uint64, len(planChs))
	minFinished := fromBlock
	startC := make(chan struct{}, 1)
	noticeF := func() {
		if number == fromBlock {
			return
		}
		for k, v := range planChs {
			select {
			case v <- number:
				noticed[k] = number
			default:
				//暂时阻塞
			}
		}
	}
	finishF := func() bool {
		if len(finished) != len(planChs) {
			return false
		}
		for _, v := range finished {
			if v < toBlock-1 {
				return false
			}
		}
		return true
	}
	fetchF := func() bool {
		//获取新的区块.
		fmt.Println("header fetchF", minFinished, number)
		//先计算
		left := number - minFinished
		//空间不够
		if max <= left {
			return false
		}
		space := max - left
		i := uint64(0)
		for ; i < space; i++ {
			if number >= toBlock {
				return finishF()
			}

			pos := posF(number)
			r, ok := rF(db, number)
			if !ok {
				return false
			}
			cache[pos] = r
			//fmt.Println("number", number, "pos", pos)
			number++
			if i > 0 && i%thread == 0 {
				noticeF()
			}
		}
		if i > 0 {
			noticeF()
		}
		return false
	}
	endF := func() {
		if number == fromBlock {
			return
		}
		for _, v := range endChs {
			select {
			case v <- number:
			}
		}
	}
	for {
		select {
		case <-endC:
			fmt.Println("-------header 结束")
			return
		case <-startC:
			fmt.Println("header startC")
			if fetchF() {
				endF()
				fmt.Println("-------header 结束")
				return
			}
		case v := <-finishedC:
			fmt.Println("header finishedC", v.index, v.number)
			if v == nil {
				return
			}
			if len(planChs) <= v.index {
				panic("planChs不一致")
			}
			finished[v.index] = v.number
			min := number
			//必须确保planChs中的每个携程都已经完成了,所以从finished中取最小的区块号.
			for _, v := range finished {
				//有一个还没有完成就直接
				if v == minFinished {
					break
				}
				if v < min {
					min = v
				}
			}
			//更新最新已经完成的,准备启动添加新的区块
			minFinished = min
			select {
			case startC <- struct{}{}:
			default:
			}
		case <-notice.C:
			fmt.Println("header noticeF")
			noticeF()
		case <-quit:
			fmt.Println("-------header 结束")
			return
		case <-timer.C:
			fmt.Println("header timer")
			ok := fetchF()
			fmt.Println(fmt.Sprintf("headers get %d ,prossed %d ", number, minFinished))
			if ok {
				endF()
				fmt.Println("-------header 结束")
				return
			}
		}
	}
}

func blockLoop(wait *sync.WaitGroup, index int, ctx context.Context, rF readF, cache []interface{}, fromBlock uint64, quit <-chan struct{}, cfg ExecuteBlockCfg, headerFinishedC chan *progress, finishedC chan *progress, planCh chan uint64, endCh chan uint64, planChs []chan uint64, endChs []chan uint64) {
	defer wait.Done()
	if len(cache) == 0 {
		return
	}
	cacheLen := uint64(len(cache))
	timer := time.NewTicker(time.Second * 2)
	notice := time.NewTicker(time.Second)
	db, err := cfg.db.BeginRo(ctx)
	if err != nil {
		panic(err)
	}
	posF := func(n uint64) uint64 {
		return (n - fromBlock) % cacheLen
	}
	number := fromBlock
	thread := uint64(runtime.NumCPU())
	finished := make(map[int]uint64, len(planChs))
	noticed := make(map[int]uint64, len(planChs))
	minFinished := fromBlock
	startC := make(chan struct{}, 1)
	noticesF := func() {
		//fmt.Println("noticesF", number)
		if number == fromBlock {
			return
		}
		for k, v := range planChs {
			select {
			case v <- number:
				noticed[k] = number
			default:
				//fmt.Println("block 暂时阻塞,通道", k)
				//暂时阻塞
			}
		}
	}
	noticeF := func(index int) {
		if number == fromBlock {
			return
		}
		select {
		case planChs[index] <- number:
			noticed[index] = number
		default:
			//fmt.Println("block 暂时阻塞,通道", index)
			//暂时阻塞
		}
	}
	toBlock := fromBlock
	end := uint64(0)
	finishF := func() bool {
		if end == fromBlock {
			return false
		}
		for _, v := range finished {
			if v < end-1 {
				return false
			}
		}
		return true
	}
	fetchF := func() bool {
		//获取新的区块.
		fmt.Println("block fetchF", minFinished, number)
		//先计算
		left := number - minFinished
		//空间不够
		if cacheLen <= left {
			return false
		}
		space := cacheLen - left
		i := uint64(0)
		for ; i < space; i++ {
			if end > 0 && number >= end {
				return finishF()
			}
			if number >= toBlock {
				return false
			}
			pos := posF(number)
			r, ok := rF(db, number)
			if !ok {
				return false
			}
			cache[pos] = r
			//fmt.Println("number", number, "pos", pos)
			number++
			if i > 0 && i%thread == 0 {
				noticesF()
			}
		}
		if i > 0 {
			noticesF()
		}
		return false
	}
	endF := func() {
		if number == fromBlock {
			return
		}
		for _, v := range endChs {
			select {
			case v <- end:
			}
		}
	}
	for {
		select {
		case <-quit:
			fmt.Println("-------block 结束")
			return
		case <-startC:
			fmt.Println("block startC")
			if fetchF() {
				endF()
				fmt.Println("-------block 结束")
				return
			}
		case e := <-planCh:
			fmt.Println("block planCh", e)
			if toBlock >= e {
				continue
			}
			toBlock = e
			select {
			case headerFinishedC <- &progress{index: index, number: e}:
			}
			fmt.Println("block planCh2", e)
			select {
			case startC <- struct{}{}:
			default:
			}
		case e := <-endCh:
			fmt.Println("block endCh")
			if e == 0 {
				continue
			}
			end = e
			toBlock = e
			endF()
		case v := <-finishedC:
			fmt.Println("block finishedC")
			if v == nil {
				fmt.Println("-------block 结束")
				return
			}
			if len(planChs) <= v.index {
				panic("planChs不一致")
			}
			fmt.Println("block 收到结束通知", v.index, "number", v.number)
			finished[v.index] = v.number
			min := number
			noticeF(v.index)
			//必须确保planChs中的每个携程都已经完成了,所以从finished中取最小的区块号.
			for _, v := range finished {
				//有一个还没有完成就直接
				if v == minFinished {
					break
				}
				if v < min {
					min = v
				}
			}
			fmt.Println("准备startc", v.index, "number", v.number)
			//更新最新已经完成的,准备启动添加新的区块
			minFinished = min
			select {
			case startC <- struct{}{}:
			default:
				fmt.Println("已经有通知了.")
			}
			fmt.Println("准备startc结束")
		case <-notice.C:
			fmt.Println("block noticesF")
			noticesF()
		case <-quit:
			fmt.Println("-------block 结束")
			return
		case <-timer.C:
			ok := fetchF()
			fmt.Println(fmt.Sprintf("block get %d ,prossed %d ", number, minFinished))
			if ok {
				endF()
				fmt.Println("-------block 结束")
				return
			}
		}
	}
}

type Result struct {
	m    map[string]map[string][]byte
	size int
}

func SpawnExecuteLoopStage(toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()
	logPrefix := "excuted"
	contractCodeCache, err := lru.New(lruDefaultSize)
	if err != nil {
		return err
	}
	var stoppedErr error
	//f, _ := os.Create("cpu_erigon.prof")
	//g, _ := os.Create("mem_erigon.prof")
	//// 打开性能分析
	//pprof.StartCPUProfile(f)
	//defer pprof.WriteHeapProfile(g)
	//defer pprof.StopCPUProfile()
	limit := runtime.NumCPU()
	c := make(chan struct{}, limit)
	finished := make(chan *info, limit)
	end := make(chan struct{})
	ttl := time.NewTicker(time.Second)
	saveTicker := time.NewTicker(time.Second)
	stageC := make(chan *Stage, 1000)
	resultC := make(chan *Result, 1000)
	fromBlock := uint64(0)
	headerCache := make([]interface{}, 100000)
	blockCache := make([]interface{}, 100000)
	fmt.Println(len(blockCache))
	planChs := make([]chan uint64, 0)
	endChs := make([]chan uint64, 0)
	progressC := make(chan *progress, 2)
	var lock sync.Mutex
	//headerLoop(ctx context.Context, cache []*types.Header, fromBlock, toBlock uint64, endC chan struct{}, cfg ExecuteBlockCfg, finishedC chan *progress, planChs []chan uint64)
	//go bodyLoop(ctx, fromBlock, end, finishedHC, cfg, blockC, numberHC)

	var saveWait sync.WaitGroup
	saveWait.Add(1)
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
	var wait sync.WaitGroup
	cacheSize := 0
	saveWait.Add(1)
	go func() {
		defer saveWait.Done()
		cache := make(map[string]map[string][]byte, 100000)
		saveF := func(data map[string]map[string][]byte, size int) {
			fmt.Println("准备存储", size)
			defer saveWait.Done()
			lock.Lock()
			defer lock.Unlock()
			if size == 0 {
				return
			}
			wtx, err := cfg.db.BeginRw(context.Background())
			if err != nil {
				panic(err)
			}
			for k, v := range data {
				for k2, v2 := range v {
					wtx.Put(k, *(*[]byte)(unsafe.Pointer(&k2)), v2)
				}
			}
			if err := wtx.Commit(); err != nil {
				panic(fmt.Sprintf("size %d,error %v", size, err))
			}
			data = nil
			fmt.Println(time.Now().String(), "写入mbdx成功.", size)
		}
		for {
			select {
			case result := <-resultC:
				if result == nil {
					return
				}
				cacheSize = cacheSize + result.size
				for k, v := range result.m {
					if _, ok := cache[k]; !ok {
						cache[k] = make(map[string][]byte)
					}
					for k2, v2 := range v {
						cache[k][k2] = v2
					}
				}
			case <-saveTicker.C:
				//if cacheSize < 256*1024*1024 {
				if cacheSize < 256*1024*1024 {
					continue
				}
				cc := cache
				saveWait.Add(1)
				fmt.Println(time.Now().String(), "准备下数据")
				go saveF(cc, cacheSize)
				cacheSize = 0
				cache = make(map[string]map[string][]byte, 100000)
			case <-end:
				saveWait.Add(1)
				saveF(cache, cacheSize)
				return
			}
		}
	}()
	stages := make([]*Stage, 0, 10)
	blockPlanCh := make(chan uint64, 1)
	blockEndCh := make(chan uint64, 1)
	stages = append(stages, NewStage(0, Headers, quit, cfg, contractCodeCache, headerCache, resultC, stageC, progressC, finished))
	for _, v := range stages {
		planChs = append(planChs, v.Plan)
		endChs = append(endChs, v.EndChan)
	}
	planChs = append(planChs, blockPlanCh)
	endChs = append(endChs, blockEndCh)
	wait.Add(2)
	go headerLoop(&wait, ctx, readHeader, headerCache, fromBlock, toBlock, quit, cfg, progressC, planChs, endChs)
	l := len(stages)
	blockProgressC := make(chan *progress, 2)
	blockFinishInfoC := make(chan *info, limit)
	outs := make([]chan uint64, 0, 10)
	//依赖后边的Senders.
	stages = append(stages, NewInStage(0, Execution, quit, cfg, contractCodeCache, blockCache, resultC, stageC, blockProgressC, blockFinishInfoC))
	for k, v := range stages {
		if k < l {
			continue
		}
		outs = append(outs, v.InputChan)
	}
	stages = append(stages, NewOutStage(1, Senders, quit, cfg, contractCodeCache, blockCache, resultC, stageC, blockProgressC, blockFinishInfoC, outs))
	blockPlanChs := make([]chan uint64, 0)
	blockEndChs := make([]chan uint64, 0)
	for k, v := range stages {
		if k < l {
			continue
		}
		blockPlanChs = append(blockPlanChs, v.Plan)
		blockEndChs = append(blockEndChs, v.EndChan)
	}
	go blockLoop(&wait, l, ctx, readHeader, blockCache, fromBlock, quit, cfg, progressC, blockProgressC, blockPlanCh, blockEndCh, blockPlanChs, blockEndChs)
	for _, v := range stages {
		wait.Add(1)
		go v.loop(&wait)
	}
	wait.Wait()
	fmt.Println("全部执行完毕,等待存储结束")
	close(end)
	saveWait.Wait()
	fmt.Println("存储结束,等待系统回收结束")
	close(finished)
	close(c)
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", fromBlock)
	return stoppedErr
}
