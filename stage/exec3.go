package stage

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
	"runtime"
	"starlink-world/erigon-evm/cmd/state/exec22"
	"starlink-world/erigon-evm/cmd/state/exec3"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/rawdb/rawdbhelpers"
	"starlink-world/erigon-evm/core/state"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/eth/ethconfig"
	"starlink-world/erigon-evm/turbo/services"
	"sync"
	"sync/atomic"
	"time"
)

//var ExecStepsInDB = metrics.NewCounter(`exec_steps_in_db`) //nolint
//var ExecRepeats = metrics.NewCounter(`exec_repeats`)       //nolint
//var ExecTriggers = metrics.NewCounter(`exec_triggers`)

var ExecStepsInDB = &atomic.Uint64{}
var ExecRepeats = &atomic.Uint64{}
var ExecTriggers = &atomic.Uint64{}

func NewProgress(prevOutputBlockNum, commitThreshold uint64, workersCount int, logPrefix string) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum, commitThreshold: commitThreshold, workersCount: workersCount, logPrefix: logPrefix}
}

type Progress struct {
	prevTime           time.Time
	prevCount          uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
	commitThreshold    uint64

	workersCount int
	logPrefix    string
}

func (p *Progress) Log(rs *state.StateV3, in *exec22.QueueWithRetry, rws *exec22.ResultsQueue, doneCount, inputBlockNum, outputBlockNum, outTxNum, repeatCount uint64, idxStepsAmountInDB float64) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	speedTx := float64(doneCount-p.prevCount) / (float64(interval) / float64(time.Second))
	//speedBlock := float64(outputBlockNum-p.prevOutputBlockNum) / (float64(interval) / float64(time.Second))
	var repeatRatio float64
	if doneCount > p.prevCount {
		repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(doneCount-p.prevCount)
	}
	log.Info(fmt.Sprintf("[%s] Transaction replay", p.logPrefix),
		//"workers", workerCount,
		"blk", outputBlockNum,
		//"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"pipe", fmt.Sprintf("(%d+%d)->%d/%d->%d/%d", in.NewTasksLen(), in.RetriesLen(), rws.ResultChLen(), rws.ResultChCap(), rws.Len(), rws.Limit()),
		"repeatRatio", fmt.Sprintf("%.2f%%", repeatRatio),
		"workers", p.workersCount,
		"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"idxStepsInDB", fmt.Sprintf("%.2f", idxStepsAmountInDB),
		//"inBlk", inputBlockNum,
		"step", fmt.Sprintf("%.1f", float64(outTxNum)/float64(ethconfig.HistoryV3AggregationStep)),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)
	//var txNums []string
	//for _, t := range rws.ResultChCap() {
	//	txNums = append(txNums, fmt.Sprintf("%d", t.TxNum))
	//}
	//s := strings.Join(txNums, ",")
	//log.Info(fmt.Sprintf("[%s] Transaction replay queue", logPrefix), "txNums", s)

	p.prevTime = currentTime
	p.prevCount = doneCount
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

func ExecV3(ctx context.Context,
	start uint64, u Unwinder, workerCount int, cfg ExecuteBlockCfg, applyTx kv.RwTx,
	parallel bool, logPrefix string,
	maxBlockNum uint64,
) error {
	batchSize, chainDb := cfg.batchSize, cfg.db
	blockReader := cfg.blockReader
	//agg, engine := cfg.agg, cfg.engine
	engine := cfg.engine
	chainConfig, genesis := cfg.chainConfig, cfg.genesis
	blockSnapshots := blockReader.(WithSnapshots).Snapshots()

	useExternalTx := applyTx != nil
	if !useExternalTx && !parallel {
		var err error
		applyTx, err = chainDb.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer applyTx.Rollback()
	} else {
		if blockSnapshots != nil && blockSnapshots.Cfg().Enabled {
			defer blockSnapshots.EnableMadvNormal().DisableReadAhead()
		}
	}

	var block, stageProgress uint64
	var maxTxNum uint64
	outputTxNum := atomic.Uint64{}
	blockComplete := atomic.Bool{}
	blockComplete.Store(true)

	var inputTxNum uint64
	if start > 0 {
		stageProgress = start
		block = start + 1
	}
	if applyTx != nil {
		//agg.SetTx(applyTx)
		//if dbg.DiscardHistory() {
		//	defer agg.DiscardHistory().FinishWrites()
		//} else {
		//	defer agg.StartWrites().FinishWrites()
		//}

		var err error
		maxTxNum, err = rawdbv3.TxNums.Max(applyTx, maxBlockNum)
		if err != nil {
			return err
		}
		if block > 0 {
			_outputTxNum, err := rawdbv3.TxNums.Max(applyTx, start)
			if err != nil {
				return err
			}
			outputTxNum.Store(_outputTxNum)
			outputTxNum.Add(1)
			inputTxNum = outputTxNum.Load()
		}
	} else {
		if err := chainDb.View(ctx, func(tx kv.Tx) error {
			var err error
			maxTxNum, err = rawdbv3.TxNums.Max(tx, maxBlockNum)
			if err != nil {
				return err
			}
			if block > 0 {
				_outputTxNum, err := rawdbv3.TxNums.Max(tx, start)
				if err != nil {
					return err
				}
				outputTxNum.Store(_outputTxNum)
				outputTxNum.Add(1)
				inputTxNum = outputTxNum.Load()
			}
			return nil
		}); err != nil {
			return err
		}
	}
	//agg.SetTxNum(inputTxNum)

	var outputBlockNum = &atomic.Uint64{}
	inputBlockNum := &atomic.Uint64{}
	var count uint64
	var lock sync.RWMutex

	rs := state.NewStateV3(cfg.dirs.Tmp)

	//TODO: owner of `resultCh` is main goroutine, but owner of `retryQueue` is applyLoop.
	// Now rwLoop closing both (because applyLoop we completely restart)
	// Maybe need split channels? Maybe don't exit from ApplyLoop? Maybe current way is also ok?

	// input queue
	in := exec22.NewQueueWithRetry(100_000)
	defer in.Close()

	rwsConsumed := make(chan struct{}, 1)
	defer close(rwsConsumed)

	execWorkers, applyWorker, rws, stopWorkers, waitWorkers := exec3.NewWorkersPool(lock.RLocker(), ctx, parallel, chainDb, rs, in, blockReader, chainConfig, genesis, engine, workerCount+1)
	defer stopWorkers()
	applyWorker.DiscardReadList()

	commitThreshold := batchSize.Bytes()
	progress := NewProgress(block, commitThreshold, workerCount, "ExecuteV3")
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	pruneEvery := time.NewTicker(2 * time.Second)
	defer pruneEvery.Stop()

	applyLoopWg := sync.WaitGroup{} // to wait for finishing of applyLoop after applyCtx cancel
	defer applyLoopWg.Wait()

	applyLoopInner := func(ctx context.Context) error {
		tx, err := chainDb.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		applyWorker.ResetTx(tx)

		var lastBlockNum uint64

		for outputTxNum.Load() <= maxTxNum {
			if err := rws.Drain(ctx); err != nil {
				return err
			}

			processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err := func() (processedTxNum uint64, conflicts, triggers int, processedBlockNum uint64, stopedAtBlockEnd bool, err error) {
				it := rws.Iter()
				defer it.Close()
				return processResultQueue(in, it, outputTxNum.Load(), rs, nil, tx, rwsConsumed, applyWorker, true, false)
			}()
			if err != nil {
				return err
			}

			ExecRepeats.Add(uint64(conflicts))
			ExecTriggers.Add(uint64(triggers))
			if processedBlockNum > lastBlockNum {
				outputBlockNum.Store(processedBlockNum)
				lastBlockNum = processedBlockNum
			}
			if processedTxNum > 0 {
				outputTxNum.Store(processedTxNum)
				blockComplete.Store(stoppedAtBlockEnd)
			}

		}
		return nil
	}
	applyLoop := func(ctx context.Context, errCh chan error) {
		defer applyLoopWg.Done()
		if err := applyLoopInner(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}
	}

	var rwLoopErrCh chan error

	var rwLoopG *errgroup.Group
	if parallel {
		// `rwLoop` lives longer than `applyLoop`
		rwLoop := func(ctx context.Context) error {
			tx, err := chainDb.BeginRw(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			//agg.SetTx(tx)
			//if dbg.DiscardHistory() {
			//	defer agg.DiscardHistory().FinishWrites()
			//} else {
			//	defer agg.StartWrites().FinishWrites()
			//}

			defer applyLoopWg.Wait()
			applyCtx, cancelApplyCtx := context.WithCancel(ctx)
			defer cancelApplyCtx()
			applyLoopWg.Add(1)
			go applyLoop(applyCtx, rwLoopErrCh)
			for outputTxNum.Load() <= maxTxNum {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case <-logEvery.C:
					stepsInDB := rawdbhelpers.IdxStepsCountV3(tx)
					progress.Log(rs, in, rws, rs.DoneCount(), inputBlockNum.Load(), outputBlockNum.Load(), outputTxNum.Load(), ExecRepeats.Load(), stepsInDB)
					//if agg.HasBackgroundFilesBuild() {
					//	log.Info(fmt.Sprintf("[%s] Background files build", logPrefix), "progress", agg.BackgroundProgress())
					//}
				case <-pruneEvery.C:
					if rs.SizeEstimate() < commitThreshold {
						//if agg.CanPrune(tx) {
						//	if err = agg.Prune(ctx, ethconfig.HistoryV3AggregationStep*10); err != nil { // prune part of retired data, before commit
						//		return err
						//	}
						//} else {
						//	if err = agg.Flush(ctx, tx); err != nil {
						//		return err
						//	}
						//}
						break
					}

					cancelApplyCtx()
					applyLoopWg.Wait()

					var t0, t1, t2, t3, t4 time.Duration
					commitStart := time.Now()
					log.Info("Committing...", "blockComplete.Load()", blockComplete.Load())
					if err := func() error {
						//Drain results (and process) channel because read sets do not carry over
						for !blockComplete.Load() {
							rws.DrainNonBlocking()
							applyWorker.ResetTx(tx)

							resIt := rws.Iter()
							processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err := processResultQueue(in, resIt, outputTxNum.Load(), rs, nil, tx, nil, applyWorker, false, true)
							if err != nil {
								return err
							}
							resIt.Close() //TODO: in defer

							ExecRepeats.Add(uint64(conflicts))
							ExecTriggers.Add(uint64(triggers))
							if processedBlockNum > 0 {
								outputBlockNum.Store(processedBlockNum)
							}
							if processedTxNum > 0 {
								outputTxNum.Store(processedTxNum)
								blockComplete.Store(stoppedAtBlockEnd)
							}
						}
						t0 = time.Since(commitStart)
						lock.Lock() // This is to prevent workers from starting work on any new txTask
						defer lock.Unlock()

						select {
						case rwsConsumed <- struct{}{}:
						default:
						}

						// Drain results channel because read sets do not carry over
						rws.DropResults(func(txTask *exec22.TxTask) {
							rs.ReTry(txTask, in)
						})

						//lastTxNumInDb, _ := rawdbv3.TxNums.Max(tx, outputBlockNum.Get())
						//if lastTxNumInDb != outputTxNum.Load()-1 {
						//	panic(fmt.Sprintf("assert: %d != %d", lastTxNumInDb, outputTxNum.Load()))
						//}

						t1 = time.Since(commitStart)
						tt := time.Now()
						if err := rs.Flush(ctx, tx, logPrefix, logEvery); err != nil {
							return err
						}
						t2 = time.Since(tt)

						//tt = time.Now()
						//if err := agg.Flush(ctx, tx); err != nil {
						//	return err
						//}
						//t3 = time.Since(tt)

						if err = tx.Put(kv.SyncStageProgress, []byte("Execution"), marshalData(outputBlockNum.Load())); nil != err {
							return err
						}

						tx.CollectMetrics()
						tt = time.Now()
						if err = tx.Commit(); err != nil {
							return err
						}
						t4 = time.Since(tt)
						for i := 0; i < len(execWorkers); i++ {
							execWorkers[i].ResetTx(nil)
						}

						return nil
					}(); err != nil {
						return err
					}
					if tx, err = chainDb.BeginRw(ctx); err != nil {
						return err
					}
					defer tx.Rollback()
					//agg.SetTx(tx)

					applyCtx, cancelApplyCtx = context.WithCancel(ctx)
					defer cancelApplyCtx()
					applyLoopWg.Add(1)
					go applyLoop(applyCtx, rwLoopErrCh)

					log.Info("Committed", "time", time.Since(commitStart), "drain", t0, "drain_and_lock", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
				}
			}
			if err = rs.Flush(ctx, tx, logPrefix, logEvery); err != nil {
				return err
			}
			//if err = agg.Flush(ctx, tx); err != nil {
			//	return err
			//}
			if err = tx.Put(kv.SyncStageProgress, []byte("Execution"), marshalData(outputBlockNum.Load())); nil != err {
				return err
			}

			if err = tx.Commit(); err != nil {
				return err
			}
			return nil
		}

		rwLoopCtx, rwLoopCtxCancel := context.WithCancel(ctx)
		defer rwLoopCtxCancel()
		rwLoopG, rwLoopCtx = errgroup.WithContext(rwLoopCtx)
		defer rwLoopG.Wait()
		rwLoopG.Go(func() error {
			defer rws.Close()
			defer in.Close()
			defer applyLoopWg.Wait()
			return rwLoop(rwLoopCtx)
		})
	}

	//if block < blockSnapshots.BlocksAvailable() {
	//	agg.KeepInDB(0)
	//	defer agg.KeepInDB(ethconfig.HistoryV3AggregationStep)
	//}

	getHeaderFunc := func(hash common.Hash, number uint64) (h *types.Header) {
		var err error
		if parallel {
			if err = chainDb.View(ctx, func(tx kv.Tx) error {
				h, err = blockReader.Header(ctx, tx, hash, number)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				panic(err)
			}
			return h
		} else {
			h, err = blockReader.Header(ctx, applyTx, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		}
	}
	if !parallel {
		applyWorker.ResetTx(applyTx)
	}

	slowDownLimit := time.NewTicker(time.Second)
	defer slowDownLimit.Stop()

	var b *types.Block
	var blockNum uint64
	var err error
Loop:
	for blockNum = block; blockNum <= maxBlockNum; blockNum++ {
		inputBlockNum.Store(blockNum)
		b, err = blockWithSenders(chainDb, applyTx, blockReader, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) common.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, engine, nil /* author */, nil /*excessDataGas*/)

		if parallel {
			select {
			case err := <-rwLoopErrCh:
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			func() {
				for rws.Len() > rws.Limit() || rs.SizeEstimate() >= commitThreshold {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-rwsConsumed:
						if !ok {
							return
						}
					case <-slowDownLimit.C:
						// fmt.Println("skip", "rws.Len()", rws.Len(), "rws.Limit()", rws.Limit(), "rws.ResultChLen()", rws.ResultChLen())
						//if tt := rws.Dbg(); tt != nil {
						//	log.Warn("fst", "n", tt.TxNum, "in.len()", in.Len(), "out", outputTxNum.Load(), "in.NewTasksLen", in.NewTasksLen())
						//}
						return
					}
				}
			}()
		}

		rules := chainConfig.Rules(blockNum, b.Time())
		var gasUsed uint64
		for txIndex := -1; txIndex <= len(txs); txIndex++ {

			// Do not oversend, wait for the result heap to go under certain size
			txTask := &exec22.TxTask{
				BlockNum:        blockNum,
				Header:          header,
				Coinbase:        b.Coinbase(),
				Uncles:          b.Uncles(),
				Rules:           rules,
				Txs:             txs,
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				BlockHash:       b.Hash(),
				SkipAnalysis:    skipAnalysis,
				Final:           txIndex == len(txs),
				GetHashFn:       getHashFn,
				EvmBlockContext: blockContext,
				Withdrawals:     b.Withdrawals(),
			}
			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					return err
				}

				if sender, ok := txs[txIndex].GetSender(); ok {
					txTask.Sender = &sender
				} else {
					sender, err := signer.Sender(txTask.Tx)
					if err != nil {
						return err
					}
					txTask.Sender = &sender
					log.Warn("[Execution] expencive lazy sender recovery", "blockNum", txTask.BlockNum, "txIdx", txTask.TxIndex)
				}
			}

			if parallel {
				if txTask.TxIndex >= 0 && txTask.TxIndex < len(txs) {
					if ok := rs.RegisterSender(txTask); ok {
						rs.AddWork(ctx, txTask, in)
					}
				} else {
					rs.AddWork(ctx, txTask, in)
				}
			} else {
				count++
				applyWorker.RunTxTask(txTask)
				if err := func() error {
					if txTask.Final {
						gasUsed += txTask.UsedGas
						if gasUsed != txTask.Header.GasUsed {
							if txTask.BlockNum > 0 { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
								return fmt.Errorf("gas used by execution: %d, in header: %d, headerNum=%d, %x", gasUsed, txTask.Header.GasUsed, txTask.Header.Number.Uint64(), txTask.Header.Hash())
							}
						}
						gasUsed = 0
					} else {
						gasUsed += txTask.UsedGas
					}
					return nil
				}(); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
						return err
					} else {
						log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", header.Hash().String(), "err", err)
						if cfg.hd != nil {
							cfg.hd.ReportBadHeaderPoS(header.Hash(), header.ParentHash)
						}
						if cfg.badBlockHalt {
							return err
						}
					}
					u.UnwindTo(blockNum-1, header.Hash())
					break Loop
				}

				if err := rs.ApplyState(applyTx, txTask, nil); err != nil {
					return fmt.Errorf("StateV3.Apply: %w", err)
				}
				ExecTriggers.Add(uint64(rs.CommitTxNum(txTask.Sender, txTask.TxNum, in)))
				outputTxNum.Add(1)

				//	if err := rs.ApplyHistory(txTask, agg); err != nil {
				//		return fmt.Errorf("StateV3.Apply: %w", err)
				//	}
			}
			stageProgress = blockNum
			inputTxNum++
		}

		if !parallel {
			outputBlockNum.Store(blockNum)

			select {
			case <-logEvery.C:
				stepsInDB := rawdbhelpers.IdxStepsCountV3(applyTx)
				progress.Log(rs, in, rws, count, inputBlockNum.Load(), outputBlockNum.Load(), outputTxNum.Load(), ExecRepeats.Load(), stepsInDB)
				if rs.SizeEstimate() < commitThreshold {
					break
				}

				var t1, t2, t3, t4 time.Duration
				commitStart := time.Now()
				if err := func() error {
					t1 = time.Since(commitStart)
					tt := time.Now()
					if err := rs.Flush(ctx, applyTx, logPrefix, logEvery); err != nil {
						return err
					}
					t2 = time.Since(tt)

					//tt = time.Now()
					//if err := agg.Flush(ctx, applyTx); err != nil {
					//	return err
					//}
					//t3 = time.Since(tt)

					if err = applyTx.Put(kv.SyncStageProgress, []byte("Execution"), marshalData(outputBlockNum.Load())); nil != err {
						return err
					}

					applyTx.CollectMetrics()

					return nil
				}(); err != nil {
					return err
				}
				log.Info("Committed", "time", time.Since(commitStart), "drain", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
			default:
			}
		}

		//if blockSnapshots.Cfg().Produce {
		//	agg.BuildFilesInBackground(outputTxNum.Load())
		//}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if parallel {
		if err := rwLoopG.Wait(); err != nil {
			return err
		}
		waitWorkers()
	} else {
		if err = rs.Flush(ctx, applyTx, logPrefix, logEvery); err != nil {
			return err
		}
		//if err = agg.Flush(ctx, applyTx); err != nil {
		//	return err
		//}
		if err = applyTx.Put(kv.SyncStageProgress, []byte("Execution"), marshalData(stageProgress)); nil != err {
			return err
		}

	}

	//if blockSnapshots.Cfg().Produce {
	//	agg.BuildFilesInBackground(outputTxNum.Load())
	//}

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func processResultQueue(in *exec22.QueueWithRetry, rws *exec22.ResultsQueueIter, outputTxNumIn uint64, rs *state.StateV3, agg *state2.AggregatorV3, applyTx kv.Tx, backPressure chan struct{}, applyWorker *exec3.Worker, canRetry, forceStopAtBlockEnd bool) (outputTxNum uint64, conflicts, triggers int, processedBlockNum uint64, stopedAtBlockEnd bool, err error) {
	var i int
	outputTxNum = outputTxNumIn
	for rws.HasNext(outputTxNum) {
		txTask := rws.PopNext()
		if txTask.Error != nil || !rs.ReadsValid(txTask.ReadLists) {
			conflicts++

			if i > 0 && canRetry {
				//send to re-exex
				rs.ReTry(txTask, in)
				continue
			}

			// resolve first conflict right here: it's faster and conflict-free
			applyWorker.RunTxTask(txTask)
			if txTask.Error != nil {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, txTask.Error
			}
			i++
		}

		if err := rs.ApplyState(applyTx, txTask, agg); err != nil {
			return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("StateV3.Apply: %w", err)
		}
		triggers += rs.CommitTxNum(txTask.Sender, txTask.TxNum, in)
		outputTxNum++
		if backPressure != nil {
			select {
			case backPressure <- struct{}{}:
			default:
			}
		}
		//if err := rs.ApplyHistory(txTask, agg); err != nil {
		//	return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("StateV3.Apply: %w", err)
		//}
		//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		processedBlockNum = txTask.BlockNum
		stopedAtBlockEnd = txTask.Final
		if forceStopAtBlockEnd && txTask.Final {
			break
		}
	}
	return
}

func blockWithSenders(db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	if tx == nil {
		tx, err = db.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, _, err = blockReader.BlockWithSenders(context.Background(), tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	return b, nil
}
