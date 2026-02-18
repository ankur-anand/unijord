package postgres

// Pulled from https://pkg.go.dev/github.com/jackc/pgx/v5@v5.8.0/pgtype
// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat

// PostgreSQL OID constants for common built-in data types.
const (
	oidBool        uint32 = 16
	oidByteA       uint32 = 17
	oidChar        uint32 = 18
	oidName        uint32 = 19
	oidInt8        uint32 = 20
	oidInt2        uint32 = 21
	oidInt4        uint32 = 23
	oidText        uint32 = 25
	oidOID         uint32 = 26
	oidXID         uint32 = 28
	oidCID         uint32 = 29
	oidJSON        uint32 = 114
	oidXML         uint32 = 142
	oidFloat4      uint32 = 700
	oidFloat8      uint32 = 701
	oidMoney       uint32 = 790
	oidMacAddr     uint32 = 829
	oidInet        uint32 = 869
	oidBPChar      uint32 = 1042
	oidVarChar     uint32 = 1043
	oidDate        uint32 = 1082
	oidTime        uint32 = 1083
	oidTimestamp   uint32 = 1114
	oidTimestampTZ uint32 = 1184
	oidInterval    uint32 = 1186
	oidTimeTZ      uint32 = 1266
	oidNumeric     uint32 = 1700
	oidUUID        uint32 = 2950
	oidJSONB       uint32 = 3802
	oidBit         uint32 = 1560
	oidVarBit      uint32 = 1562
	oidPgLSN       uint32 = 3220
	oidMacAddr8    uint32 = 774
)

var oidTypeName = map[uint32]string{
	oidBool:        "bool",
	oidByteA:       "bytea",
	oidChar:        "char",
	oidName:        "name",
	oidInt8:        "int8",
	oidInt2:        "int2",
	oidInt4:        "int4",
	oidText:        "text",
	oidOID:         "oid",
	oidXID:         "xid",
	oidCID:         "cid",
	oidJSON:        "json",
	oidXML:         "xml",
	oidFloat4:      "float4",
	oidFloat8:      "float8",
	oidMoney:       "money",
	oidMacAddr:     "macaddr",
	oidInet:        "inet",
	oidBPChar:      "bpchar",
	oidVarChar:     "varchar",
	oidDate:        "date",
	oidTime:        "time",
	oidTimestamp:   "timestamp",
	oidTimestampTZ: "timestamptz",
	oidInterval:    "interval",
	oidTimeTZ:      "timetz",
	oidNumeric:     "numeric",
	oidUUID:        "uuid",
	oidJSONB:       "jsonb",
	oidBit:         "bit",
	oidVarBit:      "varbit",
	oidPgLSN:       "pg_lsn",
	oidMacAddr8:    "macaddr8",

	// Array type OIDs. PostgreSQL names these with a leading underscore
	// (e.g. _int4 for int4[]) in pg_type.dat. The OID for each array type
	// is defined alongside its element type via the array_type_oid field.
	1000: "bool[]",
	1001: "bytea[]",
	1005: "int2[]",
	1007: "int4[]",
	1009: "text[]",
	1014: "bpchar[]",
	1015: "varchar[]",
	1016: "int8[]",
	1021: "float4[]",
	1022: "float8[]",
	1041: "inet[]",
	1115: "timestamp[]",
	1182: "date[]",
	1185: "timestamptz[]",
	1231: "numeric[]",
	2951: "uuid[]",
	3807: "jsonb[]",
}

// oidToTypeName returns the PostgreSQL type name for a given OID.
func oidToTypeName(oid uint32) string {
	if name, ok := oidTypeName[oid]; ok {
		return name
	}
	return "unknown"
}
