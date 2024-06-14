package stage

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"math/big"
	"starlink-world/erigon-evm/core/rawdb"
	"starlink-world/erigon-evm/core/types"
	"starlink-world/erigon-evm/log"
	"starlink-world/erigon-evm/turbo/services"
)

type ChainReaderImpl struct {
	config      *chain.Config
	tx          kv.Getter
	blockReader services.FullBlockReader
}

func NewChainReaderImpl(config *chain.Config, tx kv.Getter, blockReader services.FullBlockReader) *ChainReaderImpl {
	return &ChainReaderImpl{config, tx, blockReader}
}

func (cr ChainReaderImpl) Config() *chain.Config  { return cr.config }
func (cr ChainReaderImpl) CurrentHeader() *types.Header { panic("") }
func (cr ChainReaderImpl) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr ChainReaderImpl) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr ChainReaderImpl) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	if cr.blockReader != nil {
		number := rawdb.ReadHeaderNumber(cr.tx, hash)
		if number == nil {
			return nil
		}
		return cr.GetHeader(hash, *number)
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr ChainReaderImpl) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		log.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}

type EpochReaderImpl struct {
	tx kv.RwTx
}

func (cr EpochReaderImpl) GetEpoch(hash libcommon.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadEpoch(cr.tx, number, hash)
}
func (cr EpochReaderImpl) PutEpoch(hash libcommon.Hash, number uint64, proof []byte) error {
	return rawdb.WriteEpoch(cr.tx, number, hash, proof)
}
func (cr EpochReaderImpl) GetPendingEpoch(hash libcommon.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadPendingEpoch(cr.tx, number, hash)
}
func (cr EpochReaderImpl) PutPendingEpoch(hash libcommon.Hash, number uint64, proof []byte) error {
	return rawdb.WritePendingEpoch(cr.tx, number, hash, proof)
}
func (cr EpochReaderImpl) FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash libcommon.Hash, transitionProof []byte, err error) {
	return rawdb.FindEpochBeforeOrEqualNumber(cr.tx, number)
}
