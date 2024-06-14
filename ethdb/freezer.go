package ethdb

// AncientReader contains the methods required to read from immutable ancient data.
type AncientReader interface {
	// HasAncient returns an indicator whether the specified data exists in the
	// ancient store.
	HasAncient(kind string, number uint64) (bool, error)

	// Ancient retrieves an ancient binary blob from the append-only immutable files.
	Ancient(kind string, number uint64) ([]byte, error)

	// AncientRange retrieves multiple items in sequence, starting from the index 'start'.
	// It will return
	//  - at most 'count' items,
	//  - at least 1 item (even if exceeding the maxBytes), but will otherwise
	//   return as many items as fit into maxBytes.
	AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error)

	// Ancients returns the ancient item numbers in the ancient store.
	Ancients() (uint64, error)

	// Tail returns the number of first stored item in the freezer.
	// This number can also be interpreted as the total deleted item numbers.
	Tail() (uint64, error)

	// AncientSize returns the ancient size of the specified category.
	AncientSize(kind string) (uint64, error)
}

// AncientWriteOp is given to the function argument of ModifyAncients.
type AncientWriteOp interface {
	// Append adds an RLP-encoded item.
	Append(kind string, number uint64, item interface{}) error

	AppendBytes(kind string, number uint64, data []byte) error

	// AppendRaw adds an item without RLP-encoding it.
	AppendRaw(kind string, number uint64, item []byte) error
}
