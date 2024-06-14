package main

import (
	"context"
	"flag"
	"github.com/c2h5oh/datasize"
	chainp "github.com/ledgerwatch/erigon-lib/chain"
	libdir "github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"starlink-world/erigon-evm/consensus"
	"starlink-world/erigon-evm/consensus/ethash"
	"starlink-world/erigon-evm/consensus/serenity"
	"starlink-world/erigon-evm/core"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/core/vm"
	"starlink-world/erigon-evm/eth/ethconfig"
	"starlink-world/erigon-evm/ethdb/prune"
	"starlink-world/erigon-evm/interfaces"
	"starlink-world/erigon-evm/params"
	"starlink-world/erigon-evm/params/networkname"
	"starlink-world/erigon-evm/stage"
	"starlink-world/erigon-evm/turbo/snapshotsync"
	"starlink-world/erigon-evm/turbo/snapshotsync/snap"
	"sync"
	"syscall"
)

// 并发执行区块,使用mdbx,测试并发性能
var (
	datadir = "/Users/mac/devel/500s"
	//datadir                = "d:\\share\\erigon\\historyreplay\\1000s"
	startBlockNumber       = uint64(0000000)
	stopBlockNumber        = uint64(4990000)
	chaindata              = path.Join(datadir, "chaindata")
	databaseVerbosity      = int(2)
	referenceChaindata     string
	block, pruneTo, unwind uint64
	unwindEvery            uint64
	//snapDir                        = "/Volumes/1T/chaindata"
	snapDir                        = "d:\\freezer"
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
	chainName                      = networkname.MainnetChainName // Which chain to use (mainnet, ropsten, rinkeby, goerli, etc.)
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

func getBlockReader(db kv.RoDB) (blockReader interfaces.FullBlockReader) {
	openBlockReaderOnce.Do(func() {
		_blockReaderSingleton = snapshotsync.NewBlockReader()
		if sn := allSnapshots(db); sn.Cfg().Enabled {
			x := snapshotsync.NewBlockReaderWithSnapshots(sn, false)
			_blockReaderSingleton = x
		}
	})
	return _blockReaderSingleton
}

var openSnapshotOnce sync.Once
var _allSnapshotsSingleton *snapshotsync.RoSnapshots
var _aggSingleton *libstate.AggregatorV3

func allSnapshots(db kv.RoDB) *snapshotsync.RoSnapshots {
	openSnapshotOnce.Do(func() {
		var useSnapshots bool
		_ = db.View(context.Background(), func(tx kv.Tx) error {
			var err error
			useSnapshots, err = snap.Enabled(tx)
			return err
		})
		snapCfg := ethconfig.NewSnapCfg(useSnapshots, true, true)
		_allSnapshotsSingleton = snapshotsync.NewRoSnapshots(snapCfg, filepath.Join(datadir, "snapshots"))

		//var err error
		//_aggSingleton, err = libstate.NewAggregatorV3(context.Background(), filepath.Join(datadir, "snapshots", "history"), filepath.Join(datadir, "temp"), ethconfig.HistoryV3AggregationStep, db)
		//if err != nil {
		//	panic(err)
		//}
		//err = _aggSingleton.OpenFolder()
		//if err != nil {
		//	panic(err)
		//}

		if useSnapshots {
			if err := _allSnapshotsSingleton.ReopenFolder(); err != nil {
				panic(err)
			}
		}
	})
	return _allSnapshotsSingleton
}

func byChain(chain string) (*types.Genesis, *chainp.Config) {
	var chainConfig *chainp.Config
	var genesis *types.Genesis
	if chain == "" {
		chainConfig = params.MainnetChainConfig
		genesis = core.MainnetGenesisBlock()
	} else {
		chainConfig = params.ChainConfigByChainName(chain)
		genesis = core.GenesisBlockByChainName(chain)
	}
	return genesis, chainConfig
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func newSync(ctx context.Context, db kv.RwDB, miningConfig *params.MiningConfig) (prune.Mode, consensus.Engine, *chainp.Config, *vm.Config, ethconfig.Config) {
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

	genesis, chainConfig := byChain(chainName)
	genesisHash := params.GenesisHashByChainName(chainName)
	if (genesis == nil) || (genesisHash == nil) {
		log.Error("ChainDB name is not recognized: %s", chainName)
	}

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
	engine = serenity.New(engine)
	//}

	//events := privateapi.NewEvents()

	chainConfig, _, genesisErr := core.CommitGenesisBlock(db, genesis, "")
	if _, ok := genesisErr.(*chainp.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	// Apply special hacks for BSC params
	//if chainConfig.Parlia != nil {
	//	params.ApplyBinanceSmartChainParams()
	//}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.Genesis = genesis
	//cfg.TxPool.Disable = true
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}
	//if cfg.Snapshot.Enabled {
	//	// snDir := &dir.Rw{Path: filepath.Join(datadir, "snapshots")}
	//	cfg.Dirs.Snap = filepath.Join(datadir, "snapshots")
	//}

	return pm, engine, chainConfig, vmConfig, cfg
}

func main() {
	pathp := flag.String("p", "", "datadir")
	Startnum := flag.Uint64("b", 0, "begin block number")
	Stopnum := flag.Uint64("e", 0, "end block number")
	flag.Parse()
	if *pathp != "" {
		datadir = *pathp
		chaindata = path.Join(datadir, "chaindata")
	}
	if *Startnum != 0 {
		startBlockNumber = *Startnum
	}
	if *Stopnum != 0 {
		stopBlockNumber = *Stopnum
	}

	dirs := libdir.New(datadir)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler))
	// initializing the node and providing the current git commit there
	log.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	log.Info("Program starting", "args", os.Args)
	db := openDB(dirs.Chaindata, log.Root(), true)
	defer db.Close()
	ctx, _ := RootContext()
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	pm, engine, chainConfig, vmConfig, ethCfg := newSync(ctx, db, nil)

	cfg := stage.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil, false, false, true, dirs, getBlockReader(db), nil, ethCfg.Genesis, ethconfig.Sync{}, _aggSingleton)
	//go func() {
	//	http.ListenAndServe("localhost:6060", nil)
	//}()
	// stage.SpawnExecuteBlocksStage(startBlockNumber, stopBlockNumber, ctx, cfg)

	if err := stage.ExecV3(ctx, startBlockNumber, nil, 5, cfg, nil, true, "execV3", stopBlockNumber); nil != err {
		log.Error("Execv3 failed!", "error:", err)
	} else {
		log.Info("success!")
	}
}
