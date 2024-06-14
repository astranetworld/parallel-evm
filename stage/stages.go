package stage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/secp256k1"
	"runtime"
	"starlink-world/erigon-evm/common/dbutils"
	"starlink-world/erigon-evm/common/math"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/ethdb/olddb"
	"starlink-world/erigon-evm/log"
	"sync"
	"time"
	"unsafe"
)

type SyncStage string

var (
	Headers             SyncStage = "Headers"             // Headers are downloaded, their Proof-Of-Work validity and chaining is verified
	CumulativeIndex     SyncStage = "CumulativeIndex"     // Calculate how much gas has been used up to each block.
	Senders             SyncStage = "Senders"             // "From" recovered from signatures, bodies re-written
	Execution           SyncStage = "Execution"           // Executing each block w/o buildinf a trie
	IntermediateHashes  SyncStage = "IntermediateHashes"  // Generate intermediate hashes, calculate the state root hash
	AccountHistoryIndex SyncStage = "AccountHistoryIndex" // Generating history index for accounts
	StorageHistoryIndex SyncStage = "StorageHistoryIndex" // Generating history index for storage
	LogIndex            SyncStage = "LogIndex"            // Generating logs index (from receipts)
	CallTraces          SyncStage = "CallTraces"          // Generating call traces index
	TxLookup            SyncStage = "TxLookup"            // Generating transactions lookup index
	Issuance            SyncStage = "WatchTheBurn"        // Compute ether issuance for each block

	MiningCreateBlock SyncStage = "MiningCreateBlock"
	MiningExecution   SyncStage = "MiningExecution"
	MiningFinish      SyncStage = "MiningFinish"
)

// Stage is a single sync stage in staged sync.
type Stage struct {
	// Description is a string that is shown in the logs.
	Description string
	// DisabledDescription shows in the log with a message if the stage is disabled. Here, you can show which command line flags should be provided to enable the page.
	DisabledDescription string
	// Forward is called when the stage is executed. The main logic of the stage should be here. Should always end with `s.Done()` to allow going to the next stage. MUST NOT be nil!
	Forward ExecFunc
	// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
	ID SyncStage
	// Disabled defines if the stage is disabled. It sets up when the stage is build by its `StageBuilder`.
	Disabled   bool
	GasChan    chan *info
	FinishChan chan *progress
	ResultChan chan *Result
	Outs       []chan uint64
	Index      int
	Params     []interface{}
	Quit       chan struct{}
	Plan       chan uint64
	EndChan    chan uint64
	Thread     int
	StageChan  chan *Stage
	InputChan  chan uint64
	Reply      bool
}

func (s *Stage) calThreads(start, end uint64) int {
	if end <= start {
		return 1
	}
	size := int((end - start) / 10)
	if size == 0 {
		return 1
	}
	return size
}

func (s *Stage) loop(wait *sync.WaitGroup) {
	defer wait.Done()
	size := uint64(len(s.Params))
	if size == 0 {
		return
	}
	reply := uint64(math.MaxUint64)
	if s.Reply {
		reply = 0
	}
	ttl := time.NewTicker(time.Second)
	start := uint64(0)
	toBlock := uint64(0)
	thread := s.Thread
	if k := runtime.NumCPU(); thread <= k {
		thread = k
	}
	end := uint64(0)
	threads := make(chan struct{}, thread)
	finishF := func(i uint64) {
		select {
		case s.FinishChan <- &progress{index: s.Index, number: i}:
			//default:
			//fmt.Println("::通知失败.")
		}
		if len(s.Outs) > 0 {
			for _, v := range s.Outs {
				select {
				case v <- start - 1:
				}
			}
		}
	}
	runF := func() bool {
		if end > 0 && start >= toBlock {
			return true
		}
		if start >= toBlock {
			return false
		}
		fmt.Println("开始执行", start, toBlock, reply)
		if start > reply {
			return false
		}
		if reply < toBlock {
			fmt.Println("开始执行", start, toBlock, reply)
		}
		for start < toBlock && start <= reply {
			var sy sync.WaitGroup
			for i := 0; i < thread && start < toBlock && start <= reply; i++ {
				//阻塞等待.
				select {
				case threads <- struct{}{}:
					ii := start
					sy.Add(1)
					go func(ii uint64) {
						defer func() {
							if r := recover(); r != nil {
								fmt.Println("异常", ii, r)
							}
						}()
						defer func() {
							select {
							case <-threads:
							default:
							}
						}()
						defer sy.Done()
						s.Forward(s, ii, s.Params[ii%size])
					}(ii)
					start++
				case <-s.Quit:
					return true
				}
			}
			sy.Wait()
			finishF(start)
		}
		if start >= end && end > 0 {
			finishF(end - 1)
			fmt.Println("全部执行完毕.", start, end, toBlock)
			return true
		}
		fmt.Println("阶段性执行完毕.", start, end, toBlock)
		return false
	}
	newC := make(chan struct{}, 1)
	newF := func() {
		select {
		case newC <- struct{}{}:
		default:
		}
	}
	for {
		select {
		case e := <-s.EndChan:
			fmt.Println(s.StageChan, "::EndChan pos", e, start, end, toBlock)
			if e > 0 && end < e {
				end = e
				toBlock = e
			}
			newF()
		case e := <-s.InputChan:
			if s.Reply {
				fmt.Println("::InputChan", s.ID, toBlock, e)
				reply = e
				newF()
			}
		case <-s.Quit:
			fmt.Println("::Quit", s.ID, toBlock)
			fmt.Println("-------stage 结束", s.ID)
			return
		case <-newC:
			fmt.Println("::newC", s.ID, toBlock)
			if runF() {
				fmt.Println(s.StageChan, "结束", start, end, toBlock)
				fmt.Println("-------stage 结束", s.ID)
				return
			}
		case <-ttl.C:
			fmt.Println("::ttl", s.ID, toBlock)
			if runF() {
				fmt.Println(s.StageChan, "结束", start, end, toBlock)
				fmt.Println("-------stage 结束", s.ID)
				return
			}
		case n := <-s.Plan:
			fmt.Println(s.ID, "::plan pos", n, start, end, toBlock)
			if n == 0 {
				continue
			}
			toBlock = n
			newF()
		}

	}
}

type ExecFunc func(stage *Stage, number uint64, param interface{}) error

func cumulativeIndex(quit <-chan struct{}, tmpdir string, param interface{}, resultC chan *Result) error {
	header, ok := param.(*types.Header)
	if !ok || header == nil {
		return errors.New("invalid header")
	}
	batch := olddb.NewHashBatch(nil, quit, tmpdir)
	if err := rawdb.WriteTd(batch, header.Hash(), header.Number.Uint64(), header.Difficulty); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(batch, header.Hash(), header.Number.Uint64()); err != nil {
		return err
	}
	if err := rawdb.WriteHeaderNumber(batch, header.Hash(), header.Number.Uint64()); err != nil {
		return err
	}
	select {
	case resultC <- &Result{m: batch.Puts(), size: batch.BatchSize()}:
	}
	return nil
}

func senders(param interface{}, resultC chan *Result, cfg ExecuteBlockCfg) error {
	secp256k1.ContextForThread(1)
	block, ok := param.(*types.Block)
	if !ok {
		return errors.New("invalid param")
	}
	if block == nil {
		return nil
	}
	fmt.Println("senders block", block.NumberU64(), "txs", block.Transactions().Len())
	txs := block.Transactions()
	if len(txs) == 0 {
		return nil
	}
	signer := types.MakeSigner(cfg.chainConfig, block.NumberU64())
	addrs := make([]byte, 0, txs.Len()*length.Addr)
	for j := 0; j < txs.Len(); j++ {
		from, err := signer.Sender(txs[j])
		if err != nil {
			return err
		}
		addrs = append(addrs, from[:]...)
	}
	key := dbutils.EncodeBlockNumber(block.NumberU64())
	select {
	case resultC <- &Result{m: map[string]map[string][]byte{kv.Senders: {*(*string)(unsafe.Pointer(&key)): addrs}}, size: len(addrs) + len(key)}:
	}
	return nil
}

func execBlock(logPrefix string, param interface{}, resultC chan *Result, quit <-chan struct{}, cfg ExecuteBlockCfg, contractCodeCache *lru.Cache, finished chan *info) error {
	block, ok := param.(*types.Block)
	if !ok {
		return errors.New("invalid param")
	}
	if block == nil {
		return nil
	}
	fmt.Println("execBlock block", block.NumberU64(), "txs", block.Transactions().Len())
	blockNum := block.NumberU64()
	var gas uint64
	var txc uint64
	var htime uint64
	var hash libcommon.Hash
	var balance uint256.Int
	defer func() {
		select {
		case finished <- &info{gas: gas, number: blockNum, hash: hash, time: htime, balance: balance, tx: txc}:
		}
	}()
	ttx, err := cfg.db.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	htime = block.Header().Time
	//fmt.Println(blockNum, ":", arrays[i].Header.Number.Uint64())
	batch := olddb.NewHashBatch(nil, quit, cfg.dirs.Tmp)

	hash = block.Hash()
	acc, err := executeBlock(block, ttx, batch, cfg, *cfg.vmConfig)
	if err != nil {
		log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
		panic(err)
	}
	balance = acc.Balance
	gas = block.GasUsed()
	txc = uint64(block.Transactions().Len())
	//没有改变的话,就不需要这个.
	if batch.BatchSize() > 0 {
		select {
		case resultC <- &Result{m: batch.Puts(), size: batch.BatchSize()}:
		}
	}
	return nil
}

func NewInStage(index int, stage SyncStage, quit <-chan struct{}, cfg ExecuteBlockCfg, contractCodeCache *lru.Cache, params []interface{}, resultC chan *Result, stageC chan *Stage, finishChan chan *progress, gasCh chan *info) *Stage {
	s := NewStage(index, stage, quit, cfg, contractCodeCache, params, resultC, stageC, finishChan, gasCh)
	s.Reply = true
	return s
}

func NewOutStage(index int, stage SyncStage, quit <-chan struct{}, cfg ExecuteBlockCfg, contractCodeCache *lru.Cache, params []interface{}, resultC chan *Result, stageC chan *Stage, finishChan chan *progress, gasCh chan *info, outs []chan uint64) *Stage {
	s := NewStage(index, stage, quit, cfg, contractCodeCache, params, resultC, stageC, finishChan, gasCh)
	s.Outs = outs
	return s
}

func NewStage(index int, stage SyncStage, quit <-chan struct{}, cfg ExecuteBlockCfg, contractCodeCache *lru.Cache, params []interface{}, resultC chan *Result, stageC chan *Stage, finishChan chan *progress, gasCh chan *info) *Stage {
	switch stage {
	case Headers:
		return &Stage{
			ID:          stage,
			Index:       index,
			ResultChan:  resultC,
			GasChan:     gasCh,
			InputChan:   make(chan uint64, 1),
			FinishChan:  finishChan,
			StageChan:   stageC,
			Params:      params,
			EndChan:     make(chan uint64, 1),
			Plan:        make(chan uint64, 100),
			Description: "Write Cumulative Index",
			Forward: func(stage *Stage, number uint64, param interface{}) error {
				return cumulativeIndex(quit, cfg.dirs.Tmp, param, stage.ResultChan)
			},
		}
	case Senders:
		return &Stage{
			ID:          Senders,
			ResultChan:  resultC,
			Index:       index,
			GasChan:     gasCh,
			InputChan:   make(chan uint64, 1),
			FinishChan:  finishChan,
			Plan:        make(chan uint64, 1),
			EndChan:     make(chan uint64, 1),
			StageChan:   stageC,
			Params:      params,
			Description: "Recover senders from tx signatures",
			Forward: func(stage *Stage, number uint64, param interface{}) error {
				return senders(param, stage.ResultChan, cfg)
			},
		}
	case Execution:
		return &Stage{
			ID:          Execution,
			Index:       index,
			InputChan:   make(chan uint64, 1),
			ResultChan:  resultC,
			EndChan:     make(chan uint64, 1),
			GasChan:     gasCh,
			FinishChan:  finishChan,
			StageChan:   stageC,
			Plan:        make(chan uint64, 1),
			Params:      params,
			Description: "Execute blocks w/o hash checks",
			Forward: func(stage *Stage, number uint64, param interface{}) error {
				return execBlock(string(stage.ID), stage.Params, stage.ResultChan, quit, cfg, contractCodeCache, gasCh)
			},
		}
	case IntermediateHashes:
		return &Stage{
			ID:          IntermediateHashes,
			ResultChan:  resultC,
			StageChan:   stageC,
			GasChan:     gasCh,
			Index:       index,
			InputChan:   make(chan uint64, 1),
			EndChan:     make(chan uint64),
			FinishChan:  finishChan,
			Plan:        make(chan uint64),
			Params:      params,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(stage *Stage, number uint64, param interface{}) error {
				return cumulativeIndex(quit, cfg.dirs.Tmp, param, stage.ResultChan)
			},
		}

	//case CallTraces:
	//	return &Stage{
	//		ID:                  CallTraces,
	//		ResultChan:          resultC,
	//		StageChan:           stageC,
	//		Params:              params,
	//		Description:         "Generate call traces index",
	//		DisabledDescription: "Work In Progress",
	//		Forward: func(stage *Stage, param interface{}) error {
	//			return cumulativeIndex(param, stage.ResultChan)
	//		},
	//	}
	//case AccountHistoryIndex:
	//	return &Stage{
	//		ID:          AccountHistoryIndex,
	//		ResultChan:  resultC,
	//		StageChan:   stageC,
	//		Params:      params,
	//		Description: "Generate account history index",
	//		Forward: func(stage *Stage, param interface{}) error {
	//			return cumulativeIndex(param, stage.ResultChan)
	//		},
	//	}
	//case StorageHistoryIndex:
	//	return &Stage{
	//		ID:          StorageHistoryIndex,
	//		ResultChan:  resultC,
	//		StageChan:   stageC,
	//		Params:      params,
	//		Description: "Generate storage history index",
	//		Forward: func(stage *Stage, param interface{}) error {
	//			return cumulativeIndex(param, stage.ResultChan)
	//		},
	//	}
	//
	//case LogIndex:
	//	return &Stage{
	//		ID:          LogIndex,
	//		ResultChan:  resultC,
	//		StageChan:   stageC,
	//		Params:      params,
	//		Description: "Generate receipt logs index",
	//		Forward: func(stage *Stage, param interface{}) error {
	//			return cumulativeIndex(param, stage.ResultChan)
	//		},
	//	}
	//
	//case TxLookup:
	//	return &Stage{
	//		ID:          TxLookup,
	//		ResultChan:  resultC,
	//		StageChan:   stageC,
	//		Params:      params,
	//		Description: "Generate tx lookup index",
	//		Forward: func(stage *Stage, param interface{}) error {
	//			return cumulativeIndex(param, stage.ResultChan)
	//		},
	//	}
	//
	//case Issuance:
	//	return &Stage{
	//		ID:          Issuance,
	//		ResultChan:  resultC,
	//		StageChan:   stageC,
	//		Params:      params,
	//		Description: "Issuance computation",
	//		Forward: func(stage *Stage, param interface{}) error {
	//			return cumulativeIndex(param, stage.ResultChan)
	//		},
	//	}
	default:
		panic("invalid stage")
	}
}

// GetStageProgress retrieves saved progress of given sync stage from the database
func GetStageProgress(db kv.Getter, stage SyncStage) (uint64, error) {
	v, err := db.GetOne(kv.SyncStageProgress, []byte(stage))
	if err != nil {
		return 0, err
	}
	return unmarshalData(v)
}

func SaveStageProgress(db kv.Putter, stage SyncStage, progress uint64) error {
	return db.Put(kv.SyncStageProgress, []byte(stage), marshalData(progress))
}

// GetStagePruneProgress retrieves saved progress of given sync stage from the database
func GetStagePruneProgress(db kv.Getter, stage SyncStage) (uint64, error) {
	v, err := db.GetOne(kv.SyncStageProgress, []byte("prune_"+stage))
	if err != nil {
		return 0, err
	}
	return unmarshalData(v)
}

func SaveStagePruneProgress(db kv.Putter, stage SyncStage, progress uint64) error {
	return db.Put(kv.SyncStageProgress, []byte("prune_"+stage), marshalData(progress))
}

func marshalData(blockNumber uint64) []byte {
	return encodeBigEndian(blockNumber)
}

func unmarshalData(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) < 8 {
		return 0, fmt.Errorf("value must be at least 8 bytes, got %d", len(data))
	}
	return binary.BigEndian.Uint64(data[:8]), nil
}

func encodeBigEndian(n uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], n)
	return v[:]
}
