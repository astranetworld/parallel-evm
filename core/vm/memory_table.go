package vm

import "starlink-world/erigon-evm/core/vm/stack"

func memoryKeccak256(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}

func memoryCallDataCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(2))
}

func memoryReturnDataCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(2))
}

func memoryCodeCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(2))
}

func memoryExtCodeCopy(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(1), stack.Back(3))
}

func memoryMLoad(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64WithUint(stack.Back(0), 32)
}

func memoryMStore8(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64WithUint(stack.Back(0), 1)
}

func memoryMStore(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64WithUint(stack.Back(0), 32)
}

func memoryCreate(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(1), stack.Back(2))
}

func memoryCreate2(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(1), stack.Back(2))
}

func memoryCall(stack *stack.Stack) (uint64, bool) {
	x, overflow := calcMemSize64(stack.Back(5), stack.Back(6))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(stack.Back(3), stack.Back(4))
	if overflow {
		return 0, true
	}
	if x > y {
		return x, false
	}
	return y, false
}
func memoryDelegateCall(stack *stack.Stack) (uint64, bool) {
	x, overflow := calcMemSize64(stack.Back(4), stack.Back(5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(stack.Back(2), stack.Back(3))
	if overflow {
		return 0, true
	}
	if x > y {
		return x, false
	}
	return y, false
}

func memoryStaticCall(stack *stack.Stack) (uint64, bool) {
	x, overflow := calcMemSize64(stack.Back(4), stack.Back(5))
	if overflow {
		return 0, true
	}
	y, overflow := calcMemSize64(stack.Back(2), stack.Back(3))
	if overflow {
		return 0, true
	}
	if x > y {
		return x, false
	}
	return y, false
}

func memoryReturn(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}

func memoryRevert(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}

func memoryLog(stack *stack.Stack) (uint64, bool) {
	return calcMemSize64(stack.Back(0), stack.Back(1))
}
