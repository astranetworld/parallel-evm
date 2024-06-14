package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/panjf2000/ants/v2"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"starlink-world/erigon-evm/common2/dir"
	"starlink-world/erigon-evm/consensus"
	"starlink-world/erigon-evm/consensus/ethash"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/ethconfig"
	"starlink-world/erigon-evm/ethdb/prune"
	"starlink-world/erigon-evm/interfaces"
	kv2 "starlink-world/erigon-evm/kv/mdbx"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/params"
	"starlink-world/erigon-evm/stage"
	"starlink-world/erigon-evm/turbo/snapshotsync"
	"sync"
	"syscall"
	"time"
)

// 生产单个区块需要的数据,快照,以及code.
// 打包后这些数据用于提供一个精简版的evm,不再需要其他任何数据就能执行相应的区块.另外由于evm里边内部计算的时候需要查找前面一些区块的哈希,所以额外要准备一个headers.data来按序存储相应的区块头的哈希.
var (
	//todo 当前erigon的文件路径,注意不要快照!
	datadir = "/Volumes/1T/erigon"
	//datadir = "d:\\erigon"
	//todo 					运行截止区块号
	stopBlockNumber        = uint64(1000000)
	chaindata              = path.Join(datadir, "chaindata")
	databaseVerbosity      = int(2)
	referenceChaindata     string
	block, pruneTo, unwind uint64
	unwindEvery            uint64
	//todo 					 注意,这个必须填写,用于存储生产的区块信息
	snapDir = "/Volumes/1T/entire"
	//snapDir                        = "d:\\entire"
	batchSizeStr                   = "512M"
	reset                          bool
	bucket                         string
	toChaindata                    string
	migration                      string
	integrityFast                  = true
	integritySlow                  bool
	file                           string
	HeimdallURL                    string
	txtrace                        bool // Whether to trace the execution (should only be used together eith `block`)
	pruneFlag                      = "hrtc"
	pruneH, pruneR, pruneT, pruneC uint64
	pruneHBefore, pruneRBefore     uint64
	pruneTBefore, pruneCBefore     uint64
	experiments                    []string
	chain                          string // Which chain to use (mainnet, ropsten, rinkeby, goerli, etc.)
	syncmodeStr                    string
)

func openKV(label kv.Label, logger log.Logger, path string, exclusive bool) kv.RwDB {
	opts := kv2.NewMDBX(logger).Path(path).Label(label)
	//opts := kv2.NewMDBX(logger).Freezer(snapDir).NoBaseFreezerReadable(false).Path(path).Label(label)
	if exclusive {
		opts = opts.Exclusive()
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts.MustOpen()
}

func openDB(path string, logger log.Logger, applyMigrations bool) kv.RwDB {
	label := kv.ChainDB
	db := openKV(label, logger, path, false)
	if applyMigrations {
		//has, err := migrations.NewMigrator(label).HasPendingMigrations(db)
		//if err != nil {
		//	panic(err)
		//}
		//if has {
		//	log.Info("Re-Opening DB in exclusive mode to apply DB migrations")
		//	db.Close()
		//	db = openKV(label, logger, path, true)
		//	if err := migrations.NewMigrator(label).Apply(db, datadir); err != nil {
		//		panic(err)
		//	}
		//	db.Close()
		//	db = openKV(label, logger, path, false)
		//}
	}
	return db
}

func RootContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()

		ch := make(chan os.Signal, 1)
		defer close(ch)

		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case sig := <-ch:
			log.Info("Got interrupt, shutting down...", "sig", sig)
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

var openBlockReaderOnce sync.Once
var _blockReaderSingleton interfaces.FullBlockReader

func getBlockReader(cc *chain.Config) (blockReader interfaces.FullBlockReader) {
	openBlockReaderOnce.Do(func() {
		_blockReaderSingleton = snapshotsync.NewBlockReader()
	})
	return _blockReaderSingleton
}

func byChain(chain string) (*core.Genesis, *chain.Config) {
	var chainConfig *chain.Config
	var genesis *core.Genesis
	if chain == "" {
		chainConfig = params.MainnetChainConfig
		genesis = core.DefaultGenesisBlock()
	} else {
		chainConfig = params.ChainConfigByChainName(chain)
		genesis = core.DefaultGenesisBlockByChainName(chain)
	}
	return genesis, chainConfig
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func newSync(ctx context.Context, db kv.RwDB, miningConfig *params.MiningConfig) (prune.Mode, consensus.Engine, *chain.Config, *vm.Config) {
	//tmpdir := filepath.Join(datadir, etl.TmpDirName)
	//logger := log.New()

	var pm prune.Mode
	var err error
	if err = db.View(context.Background(), func(tx kv.Tx) error {
		pm, err = prune.Get(tx)
		if err != nil {
			return err
		}
		//if err = stagedsync.UpdateMetrics(tx); err != nil {
		//	return err
		//}
		return nil
	}); err != nil {
		panic(err)
	}
	vmConfig := &vm.Config{}

	genesis, chainConfig := byChain(chain)
	var engine consensus.Engine
	//config := &ethconfig.Defaults
	//if chainConfig.Clique != nil {
	//	c := params.CliqueSnapshot
	//	c.DBPath = filepath.Join(datadir, "clique", "db")
	//	engine = ethconfig.CreateConsensusEngine(chainConfig, logger, c, config.Miner.Notify, config.Miner.Noverify, "", true, datadir)
	//} else if chainConfig.Aura != nil {
	//	engine = ethconfig.CreateConsensusEngine(chainConfig, logger, &params.AuRaConfig{DBPath: filepath.Join(datadir, "aura")}, config.Miner.Notify, config.Miner.Noverify, "", true, datadir)
	//} else if chainConfig.Parlia != nil {
	//	consensusConfig := &params.ParliaConfig{DBPath: filepath.Join(datadir, "parlia")}
	//	engine = ethconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "", true, datadir)
	//} else if chainConfig.Bor != nil {
	//	consensusConfig := &config.Bor
	//	engine = ethconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "http://localhost:1317", false, datadir)
	//} else { //ethash
	engine = ethash.NewFaker()
	//}

	//events := privateapi.NewEvents()

	chainConfig, _, genesisErr := core.CommitGenesisBlock(db, genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	// Apply special hacks for BSC params
	if chainConfig.Parlia != nil {
		params.ApplyBinanceSmartChainParams()
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.TxPool.Disable = true
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}
	if cfg.Snapshot.Enabled {
		snDir := &dir.Rw{Path: filepath.Join(datadir, "snapshots")}
		cfg.SnapshotDir = snDir
	}

	return pm, engine, chainConfig, vmConfig
}

func main() {
	logger := log.New()
	db := openDB(chaindata, logger, true)
	defer db.Close()
	ctx, _ := RootContext()
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	pm, engine, chainConfig, vmConfig := newSync(ctx, db, nil)
	tmpdir := filepath.Join(datadir, etl.TmpDirName)
	cfg := stage.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil, false, tmpdir, getBlockReader(chainConfig))
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	fmt.Println(cfg)
	os.Remove(filepath.Join(snapDir, "headers.dat"))
	f, err := os.OpenFile(filepath.Join(snapDir, "headers.dat"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	tx, err := db.BeginRo(ctx)
	defer tx.Rollback()
	if err != nil {
		panic(err)
	}
	buff := bufio.NewWriter(f)
	fmt.Println("开始写入区块头的hash.")
	for i := uint64(0); i < stopBlockNumber; i++ {
		hash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			panic(err)
		}
		buff.Write(hash.Bytes())
	}
	buff.Flush()
	fmt.Println("写入区块头的hash完毕.")
	var a sync.WaitGroup
	pool, err := ants.NewPoolWithFunc(1000, func(i interface{}) {
		defer a.Done()
		stage.GenEntireCode(i.(uint64), snapDir, ctx, cfg)
	})
	if err != nil {
		panic(err)
	}
	now0, now1 := time.Now(), time.Now()
	//numbers := []uint64{3158467, 3158579, 3158568, 3159169, 3726737}
	//for _, i := range numbers {
	for i := uint64(1); i < stopBlockNumber+1; i++ {
		//if st, err := os.Stat(path.Join(snapDir, fmt.Sprintf("%d.block", i))); err == nil && st != nil {
		//	continue
		//}
		ii := i
		a.Add(1)
		//fmt.Println(ii)
		for {
			if err := pool.Invoke(ii); err == nil {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		if n := time.Now(); n.Sub(now1) > time.Second {
			fmt.Println("准备数据", "number", ii, "耗时", n.Sub(now1), "总耗时", n.Sub(now0))
			now1 = time.Now()
		}
	}
	fmt.Println("全部提交", "总耗时", time.Now().Sub(now0))
	a.Wait()
	fmt.Println("全部完成", "总耗时", time.Now().Sub(now0))
}
