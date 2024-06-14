package core

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// See https://github.com/ethereum/execution-apis/blob/main/src/engine/specification.md#payloadattributesv1
type BlockProposerParametersPOS struct {
	ParentHash            libcommon.Hash
	Timestamp             uint64
	PrevRandao            libcommon.Hash
	SuggestedFeeRecipient libcommon.Address
}
