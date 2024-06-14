package mdbx

const (
	// freezerHeaderTable indicates the name of the freezer header table.
	freezerHeaderTable = "headers"

	// freezerHashTable indicates the name of the freezer canonical hash table.
	freezerHashTable = "hashes"

	// freezerBodiesTable indicates the name of the freezer block body table.
	freezerBodiesTable = "bodies"

	// freezerReceiptTable indicates the name of the freezer receipts table.
	freezerReceiptTable = "receipts"

	// freezerSenderTable indicates the name of the freezer senders table.
	freezerSenderTable = "senders"
	// freezerTransactionsTable indicates the name of the freezer transactions table.
	freezerTransactionsTable = "transactions"
	// freezerEntiresTable indicates the name of the freezer entire.
	freezerEntiresTable = "entire"
)

// FreezerNoSnappy configures whether compression is disabled for the ancient-tables.
// Hashes and difficulties don't compress well.
var FreezerNoSnappy = map[string]bool{
	freezerHeaderTable:       false,
	freezerHashTable:         true,
	freezerBodiesTable:       false,
	freezerReceiptTable:      false,
	freezerSenderTable:       false,
	freezerTransactionsTable: false,
}

var SnapFreezerNoSnappy = map[string]bool{
	freezerEntiresTable: false,
}
