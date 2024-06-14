package params

// These are the multipliers for ether denominations.
// Example: To get the wei value of an amount in 'gwei', use
//
//	new(big.Int).Mul(value, big.NewInt(params.GWei))
const (
	Wei   = 1
	GWei  = 1e9
	Ether = 1e18
)
