package vm

import (
	"starlink-world/erigon-evm/params"
)

func maxStack(pop, push int) int {
	return int(params.StackLimit) + pop - push
}
