package stage

import (
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/panjf2000/ants/v2"
	"os"
	"runtime"
	"runtime/pprof"
	"starlink-world/erigon-evm/common"
	common2 "starlink-world/erigon-evm/common2"
	"starlink-world/erigon-evm/consensus"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/types/accounts"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/calltracer"
	"starlink-world/erigon-evm/eth/ethconfig"
	"starlink-world/erigon-evm/eth/tracers/logger"
	"starlink-world/erigon-evm/ethdb"
	"starlink-world/erigon-evm/ethdb/olddb"
	"starlink-world/erigon-evm/ethdb/prune"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/turbo/services"
	"starlink-world/erigon-evm/turbo/shards"
	"starlink-world/erigon-evm/turbo/snapshotsync"
	"sync"
	"time"
)

const (
	lruDefaultSize = 1_000_000 // 56 MB
	logInterval    = 1 * time.Second
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type headerDownloader interface {
	ReportBadHeaderPoS(badHeader, lastValidAncestor libcommon.Hash)
}

type WithSnapshots interface {
	Snapshots() *snapshotsync.RoSnapshots
}

type ExecuteBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook ChangeSetHook
	chainConfig   *chain.Config
	engine        consensus.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	accumulator   *shards.Accumulator
	blockReader   services.FullBlockReader
	hd            headerDownloader

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis
	agg       *libstate.AggregatorV3
}

func StageExecuteBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook ChangeSetHook,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.Config,
	accumulator *shards.Accumulator,
	stateStream bool,
	badBlockHalt bool,

	historyV3 bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.AggregatorV3,
) ExecuteBlockCfg {
	return ExecuteBlockCfg{
		db:            db,
		prune:         pm,
		batchSize:     batchSize,
		changeSetHook: changeSetHook,
		chainConfig:   chainConfig,
		engine:        engine,
		vmConfig:      vmConfig,
		dirs:          dirs,
		accumulator:   accumulator,
		stateStream:   stateStream,
		badBlockHalt:  badBlockHalt,
		blockReader:   blockReader,
		hd:            hd,
		genesis:       genesis,
		historyV3:     historyV3,
		syncCfg:       syncCfg,
		agg:           agg,
	}
}

func executeBlock(
	block *types.Block,
	tx kv.Tx,
	batch ethdb.Database,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
) (*accounts.Account, error) {
	blockNum := block.NumberU64()
	//stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, initialCycle, cfg.stateStream)
	//if err != nil {
	//	return nil, err
	//}

	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets
	var err error
	stateReader = NewStateHistoryReader(tx, batch, blockNum)
	//stateReader = state.NewPlainState(tx, blockNum, nil)
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

	//var receipts types.Receipts
	//var stateSyncReceipt *types.Receipt
	//var execRs *core.EphemeralExecResult
	//_, isPoSa := cfg.engine.(consensus.PoSA)
	isBor := cfg.chainConfig.Bor != nil

	getHashFn := core.GetHashFn(block.Header(), getHeader)
	//if isPoSa {
	//	_, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, EpochReaderImpl{tx: nil}, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	//} else
	if isBor {
		_, err = core.ExecuteBlockEphemerallyBor(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	} else {
		_, err = core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer)
	}
	if err != nil {
		return nil, err
	}
	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	acc, err := stateReader.ReadAccountData(block.Header().Coinbase)
	if err != nil {
		return nil, err
	}
	return acc, err
}

func newStateReaderWriter(
	batch ethdb.Database,
	tx kv.RwTx,
	block *types.Block,
	writeChangesets bool,
	accumulator *shards.Accumulator,
	initialCycle bool,
	stateStream bool,
) (state.StateReader, state.WriterWithChangeSets, error) {

	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	stateReader = state.NewPlainStateReader(batch)

	if !initialCycle && stateStream {
		txs, err := rawdb.RawTransactionsRange(tx, block.NumberU64(), block.NumberU64())
		if err != nil {
			return nil, nil, err
		}
		accumulator.StartChange(block.NumberU64(), block.Hash(), txs, false)
	} else {
		accumulator = nil
	}
	if writeChangesets {
		stateWriter = state.NewPlainStateWriter(batch, tx, block.NumberU64()).SetAccumulator(accumulator)
	} else {
		stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(accumulator)
	}

	return stateReader, stateWriter, nil
}
func readBlock(tx kv.Getter, ctx context.Context, cfg ExecuteBlockCfg, blockNum uint64, logPrefix string) (*types.Block, error) {
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	block, _, err := cfg.blockReader.BlockWithSenders(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		//log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
		return nil, nil
	}
	return block, nil
}

type info struct {
	gas     uint64
	tx      uint64
	number  uint64
	time    uint64
	balance uint256.Int
	hash    libcommon.Hash
	snap    []byte
}

func comma18(s string) string {
	n := len(s)
	if n <= 18 {
		return s
	}
	return (s[:n-18]) + "." + s[n-18:]
}

func SpawnExecuteBlocksStage(startBlock, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()

	//prevStageProgress, errStart := stages.GetStageProgress(tx, stages.Senders)
	//if errStart != nil {
	//	return errStart
	//}
	//nextStageProgress, err := stages.GetStageProgress(tx, stages.HashState)
	//if err != nil {
	//	return err
	//}
	//nextStagesExpectData := nextStageProgress > 0 // Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
	//nextStagesExpectData := false
	logPrefix := "excuted"
	//to := stopBlockNumber
	//var to = prevStageProgress
	//if toBlock > 0 {
	//	to = min(prevStageProgress, toBlock)
	//}
	//if to <= s.BlockNumber {
	//	return nil
	//}
	//if to > s.BlockNumber+16 {
	//	log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	//}

	//startTime := time.Now()

	//whitelistedTables := []string{kv.Code, kv.ContractCode}
	// Contract code is unlikely to change too much, so let's keep it cached
	//contractCodeCache, err := lru.New(lruDefaultSize)

	// changes are stored through memory buffer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	//logBlock := stageProgress
	//logTx, lastLogTx := uint64(0), uint64(0)
	//logTime := time.Now()
	//var gas uint64             // used for logs
	//var currentStateGas uint64 // used for batch commits of state
	//// Transform batch_size limit into Ggas
	//gasState := uint64(cfg.batchSize) * uint64(datasize.KB) * 2

	//startGasUsed, err := rawdb.ReadCumulativeGasUsed(tx, s.BlockNumber)
	//if err != nil {
	//	return err
	//}
	//totalGasUsed := new(big.Int).SetUint64(0)
	//totalGasUsed, err := rawdb.ReadCumulativeGasUsed(tx, to)
	//if err != nil {
	//	return err
	//}
	var stoppedErr error
	f, _ := os.Create("cpu_erigon.prof")
	g, _ := os.Create("mem_erigon.prof")
	//// 打开性能分析
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
		var GasUsedSec, ttx, ttx1, number, numberp uint64
		numberp = startBlock
		//var hash libcommon.Hash
		//var balance uint256.Int
		//var htime uint64
		start := time.Now()
		for {
			select {
			case cc := <-finished:
				if cc == nil {
					return
				}
				ttx1 += cc.tx
				ttx += cc.tx
				number = cc.number
				//htime = cc.time
				//hash = cc.hash
				//balance = cc.balance
				GasUsedSec += cc.gas
				select {
				case <-c:
				}
			case <-ttl.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				fmt.Printf("blocks %6d txs %6d blk/s %3d tx/s %4d Mgas/s %5d time %7v\n",
					number, ttx1, number-numberp, ttx, GasUsedSec>>20, common.PrettyDuration(time.Since(start)))
				//				fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %6d hash %s %26s age %9s time %v alloc %s sys %s\n",
				//					number, ttx1, GasUsedSec>>20, ttx, hash.TerminalString(), comma18(balance.ToBig().String()),
				//					common.PrettyAge(time.Unix(int64(htime), 0)), common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
				GasUsedSec = 0
				numberp = number
				ttx = 0
			case <-end:
				return

			}
		}
	}()
	blockNum := uint64(startBlock)
	var w sync.WaitGroup
	p, _ := ants.NewPoolWithFunc(1000, func(i interface{}) {
		blockNum := i.(uint64)
		var gas uint64
		var txc uint64
		var htime uint64
		var hash libcommon.Hash
		var balance uint256.Int
		defer func() {
			select {
			case finished <- &info{gas: gas, number: blockNum, hash: hash, time: htime, balance: balance, tx: txc}:
			}
			w.Done()
		}()
		var batch ethdb.DbWithPendingMutations
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
		//fmt.Println(blockNum, ":", arrays[i].Header.Number.Uint64())
		// state is stored through ethdb batches
		batch = olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)

		defer batch.Rollback()

		hash = block.Hash()
		acc, err := executeBlock(block, ttx, batch, cfg, *cfg.vmConfig)
		if err != nil {
			log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
			panic(err)
		}
		if acc == nil {
			fmt.Printf("%+v", acc)
		}
		balance = acc.Balance
		gas = block.GasUsed()
		txc = uint64(block.Transactions().Len())
		//fmt.Println("区块结束", blockNum)
	})
	for ; blockNum < toBlock; blockNum++ {
		//fmt.Println("排队", blockNum)
		select {
		case c <- struct{}{}:
		}
		w.Add(1)
		//fmt.Println("进入", blockNum)

		i := blockNum
		p.Invoke(i)
		//go func(blockNum uint64) {
		//	var gas uint64
		//	var txc uint64
		//	var htime uint64
		//	var hash libcommon.Hash
		//	var balance uint256.Int
		//	defer func() {
		//		select {
		//		case finished <- &info{gas: gas, number: blockNum, hash: hash, time: htime, balance: balance, tx: txc}:
		//		}
		//		w.Done()
		//	}()
		//	var batch ethdb.DbWithPendingMutations
		//	ttx, err := cfg.db.BeginRo(context.Background())
		//	if err != nil {
		//		panic(err)
		//	}
		//	defer ttx.Rollback()
		//	block, err := readBlock(ttx, ctx, cfg, blockNum, logPrefix)
		//	if err != nil {
		//		panic(err)
		//	}
		//	if block == nil {
		//		log.Error("区块不存在", "blocknumber", blockNum)
		//		panic("区块不足")
		//	}
		//	htime = block.Header().Time
		//	//fmt.Println(blockNum, ":", arrays[i].Header.Number.Uint64())
		//	// state is stored through ethdb batches
		//	batch = olddb.NewHashBatch(nil, quit, cfg.tmpdir, whitelistedTables, contractCodeCache)
		//
		//	defer batch.Rollback()
		//
		//	var contractHasTEVM func(contractHash libcommon.Hash) (bool, error)
		//
		//	if cfg.vmConfig.EnableTEMV {
		//		contractHasTEVM = ethdb.GetHasTEVM(ttx)
		//	}
		//	if blockNum == printNumber {
		//		//fmt.Println("开始准备测试断点")
		//	}
		//	hash = block.Hash()
		//	acc, err := executeBlock(block, ttx, batch, cfg, *cfg.vmConfig, contractHasTEVM)
		//	if err != nil {
		//		log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
		//		panic(err)
		//	}
		//	balance = acc.Balance
		//	gas = block.GasUsed()
		//	txc = uint64(block.Transactions().Len())
		//	//fmt.Println("区块结束", blockNum)
		//}(i)
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

type Sync struct {
	unwindPoint     *uint64 // used to run stages
	prevUnwindPoint *uint64 // used to get value from outside of staged sync after cycle (for example to notify RPCDaemon)
	badBlock        libcommon.Hash

	stages       []*Stage
	unwindOrder  []*Stage
	pruningOrder []*Stage
	currentStage uint
	timings      []Timing
	logPrefixes  []string
}

func (s *Sync) Len() int                 { return len(s.stages) }
func (s *Sync) PrevUnwindPoint() *uint64 { return s.prevUnwindPoint }
func (s *Sync) LogPrefix() string {
	if s == nil {
		return ""
	}
	return s.logPrefixes[s.currentStage]
}
func (s *Sync) SetCurrentStage(id SyncStage) error {
	for i, stage := range s.stages {
		if stage.ID == id {
			s.currentStage = uint(i)
			return nil
		}
	}
	return fmt.Errorf("stage not found with id: %v", id)
}

type Timing struct {
	isUnwind bool
	isPrune  bool
	stage    Stage
	took     time.Duration
}

// StageState is the state of the stage.
type StageState struct {
	state       *Sync
	ID          SyncStage
	BlockNumber uint64 // BlockNumber is the current block number of the stage at the beginning of the state execution.
}

func (s *StageState) LogPrefix() string { return s.state.LogPrefix() }

// Unwinder allows the stage to cause an unwind.
type Unwinder interface {
	// UnwindTo begins staged sync unwind to the specified block.
	UnwindTo(unwindPoint uint64, badBlock libcommon.Hash)
}

func SpawnExecuteBlocksStageS(toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	tx, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	logPrefix := "excuted"
	startBlock := uint64(0)
	quit := ctx.Done()
	var to = toBlock
	var ttx1, ttx uint64

	startTime := time.Now()

	var batch ethdb.DbWithPendingMutations
	// state is stored through ethdb batches
	batch = olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)

	defer batch.Rollback()
	// changes are stored through memory buffer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := startBlock
	var gas uint64             // used for logs
	var currentStateGas uint64 // used for batch commits of state
	// Transform batch_size limit into Ggas
	gasState := uint64(cfg.batchSize) * uint64(datasize.KB) * 2

	if err != nil {
		return err
	}
	var stoppedErr error

	//effectiveEngine := cfg.engine	//if asyncEngine, ok := effectiveEngine.(consensus.AsyncEngine); ok {
	//	asyncEngine = asyncEngine.WithExecutionContext(ctx)
	//	effectiveEngine = asyncEngine.(consensus.Engine)
	//}

	for blockNum := startBlock; blockNum <= to; blockNum++ {
		if stoppedErr = common2.Stopped(quit); stoppedErr != nil {
			break
		}

		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		block, _, err := cfg.blockReader.BlockWithSenders(ctx, tx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			break
		}
		ttx += uint64(block.Transactions().Len())
		ttx1 += uint64(block.Transactions().Len())

		acc, err := executeBlock(block, tx, batch, cfg, *cfg.vmConfig)
		if err != nil {
			log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
			panic(err)
		}
		stageProgress = blockNum

		if currentStateGas >= gasState {
			log.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState)
			currentStateGas = 0
		}

		gas = gas + block.GasUsed()
		currentStateGas = currentStateGas + block.GasUsed()
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %5d hash %s %26s age %9s time %v alloc %s sys %s\n",
				blockNum, ttx1, gas>>20, ttx, block.Hash().TerminalString(), comma18(acc.Balance.ToBig().String()),
				common.PrettyAge(time.Unix(int64(block.Time()), 0)), common.PrettyDuration(time.Since(startTime)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
			log.Info(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
			gas = 0
			ttx = 0
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	return stoppedErr
}

func logProgress(logPrefix string, prevBlock uint64, prevTime time.Time, currentBlock uint64, prevTx, currentTx uint64, gas uint64, gasState float64, estimatedTime common.PrettyDuration, batch ethdb.DbWithPendingMutations) (uint64, uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentBlock-prevBlock) / (float64(interval) / float64(time.Second))
	speedTx := float64(currentTx-prevTx) / (float64(interval) / float64(time.Second))
	speedMgas := float64(gas) / 1_000_000 / (float64(interval) / float64(time.Second))

	var m runtime.MemStats
	common2.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentBlock,
		"blk/s", fmt.Sprintf("%.1f", speed),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"Mgas/s", fmt.Sprintf("%.1f", speedMgas),
		"gasState", fmt.Sprintf("%.2f", gasState),
	}
	if estimatedTime > 0 {
		logpairs = append(logpairs, "estimated duration", estimatedTime)
	}
	if batch != nil {
		logpairs = append(logpairs, "batch", common2.ByteCount(uint64(batch.BatchSize())))
	}
	logpairs = append(logpairs, "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
	log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix), logpairs...)

	return currentBlock, currentTx, currentTime
}

func SpawnExecuteBlocksStageM(toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	quit := ctx.Done()
	logPrefix := "excuted"
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var stoppedErr error
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
				fmt.Printf("blocks %6d txs %6d Mgas/s %4d tx/s %5d elapsed %9v hash %s %26s age %9s time %v alloc %s sys %s\n",
					number, ttx1, GasUsedSec>>20, ttx, common.PrettyDuration(time.Since(start)), hash.TerminalString(), comma18(balance.ToBig().String()),
					common.PrettyAge(time.Unix(int64(htime), 0)), common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
				GasUsedSec = 0
				start = time.Now()
				ttx = 0
			case <-end:
				return
			}
		}
	}()
	blockNum := uint64(0)
	var w sync.WaitGroup
	for ; blockNum < toBlock; blockNum++ {
		select {
		case c <- struct{}{}:
		}
		w.Add(1)
		i := blockNum
		var gas uint64
		var txc uint64
		var htime uint64
		var hash libcommon.Hash
		var balance uint256.Int
		go func(blockNum uint64) {
			defer func() {
				select {
				case finished <- &info{gas: gas, number: blockNum, hash: hash, time: htime, balance: balance, tx: txc}:
				}
				w.Done()
			}()
			var batch ethdb.DbWithPendingMutations
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
			batch = olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)
			defer batch.Rollback()

			hash = block.Hash()
			acc, err := executeBlock(block, ttx, batch, cfg, *cfg.vmConfig)
			if err != nil {
				log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
				panic(err)
			}
			balance = acc.Balance
			gas = block.GasUsed()
			txc = uint64(block.Transactions().Len())
		}(i)
		if blockNum > toBlock {
			break
		}
	}
	w.Wait()
	close(end)
	close(finished)
	close(c)
	return stoppedErr
}
