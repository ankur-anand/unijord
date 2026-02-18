package postgres

// Operation that is carried out at postgres level
type Operation string

const (
	OpCreate Operation = "c"
	OpRead   Operation = "r"
	OpUpdate Operation = "u"
	OpDelete Operation = "d"

	OpTruncate Operation = "t"
)

// Column describes one table column in the CDC Stream.
type Column struct {
	Name         string
	TypeOID      uint32
	TypeName     string
	IsPrimaryKey bool
}

// ColumnTypeMetadata is used for preserving the original PostgresSQL type identity for
// each column in the CDC event envelope.
// For Example: OID: 23 will be int4 which is basically the postgres OID.
// Format is currently just a placeholder, probably a good place for the toast value.
type ColumnTypeMetadata struct {
	OID     uint32 `json:"oid"`
	Name    string `json:"name"`
	Format  string `json:"format"`
	Unknown bool   `json:"unknown,omitempty"`
}

// BEGIN;
// INSERT INTO public.users (id, name) Values (100, 'ankur'); -- EVENT 1
// INSERT INTO public.orders (id, user_id, amount) VALUES (500, 100, 29.99); -- EVENT 2
// INSERT INTO public.orders (id, user_id, amount) VALUES (501, 100, 49.99); -- EVENT 3
// UPDATE public.users SET name = 'ankur_anand' WHERE id = 100; -- EVENT 4
// COMMIT;
// Since we are using isledb for each event to be stored in the BlobStore
// we will use the TotalOrder and DataCollectionOrder for filtering table level
// order in the Txn.
// This is needed for us because we dont want to divide into multiple parallel topic
// like Kafka and loose cross table Txn Semantics. This keep our operation simple.
// Table 			TotalOrder	DataCollectionOrder	Meaning
//	public.users	1	1	1st event overall, 1st event for users
//	public.orders	2	1	2nd event overall, 1st event for orders
//	public.orders	3	2	3rd event overall, 2nd event for orders
//	public.users	4	2	4th event overall, 2nd event for users

type TransactionInfo struct {
	ID         string `json:"id"`
	TotalOrder int    `json:"total_order"`
	// DataCollectionOrder is a per-table counter.
	// See example above
	DataCollectionOrder int `json:"data_collection_order"`
}

type EnvelopePayload struct {
	Before      map[string]any                `json:"before"`
	After       map[string]any                `json:"after"`
	Source      map[string]any                `json:"source"`
	Op          Operation                     `json:"op"`
	TsMS        int64                         `json:"ts_m_sec"`
	TsUSec      int64                         `json:"ts_u_sec,omitempty"`
	ColumnTypes map[string]ColumnTypeMetadata `json:"column_types,omitempty"`
	Transaction *TransactionInfo              `json:"transaction,omitempty"`
}

// Envelope is the value
type Envelope struct {
	Payload EnvelopePayload `json:"payload"`
}
