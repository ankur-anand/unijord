package postgres

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
)

// maxSafeJSONInt is 2^53 — the largest integer that can round-trip through
// JSON → float64 → int64 without precision loss.  Any int8 value whose
// absolute value exceeds this threshold is stored as a JSON string so that
// consumers can parse it losslessly (e.g. via json.Number or strconv).
const maxSafeJSONInt = 1 << 53

// Decoder decodes pgoutput tuple data into Go maps.
type Decoder struct {
	relations        *RelationCache
	toastPlaceholder string
	filter           FilterConfig
}

// NewDecoder creates a pgoutput decoder with the given config.
func NewDecoder(relations *RelationCache, cfg Config) *Decoder {
	return &Decoder{
		relations:        relations,
		toastPlaceholder: cfg.Toast.PlaceholderValue(),
		filter:           cfg.Tables,
	}
}

// TupleDataColumn represents a single column in a pgoutput tuple.
type TupleDataColumn struct {
	// DataType: 'n' = null, 'u' = unchanged TOAST, 't' = text
	DataType byte
	Data     []byte
}

// DecodeTuple converts a slice of TupleDataColumn into a map of column name → Go value.
// All values are decoded from typoutput format representation
// The resulting Go types are chosen to be
// JSON-safe. they survive json.Marshal → json.Unmarshal round-trips without
// data loss or precision issues.
func (d *Decoder) DecodeTuple(
	relOID uint32,
	columns []TupleDataColumn,
) (map[string]any, map[string]ColumnTypeMetadata, error) {
	rel, ok := d.relations.Get(relOID)
	if !ok {
		return nil, nil, fmt.Errorf("unknown relation OID %d", relOID)
	}

	if len(columns) != len(rel.Columns) {
		return nil, nil, fmt.Errorf(
			"column count mismatch for %s.%s: got %d, expected %d",
			rel.Schema, rel.Name, len(columns), len(rel.Columns))
	}

	row := make(map[string]any, len(columns))
	types := make(map[string]ColumnTypeMetadata, len(columns))

	for i, col := range columns {
		colDef := rel.Columns[i]

		if d.filter.ColumnExcluded(rel.Schema, rel.Name, colDef.Name) {
			continue
		}

		switch col.DataType {
		// NULL
		case 'n':
			row[colDef.Name] = nil
			types[colDef.Name] = ColumnTypeMetadata{
				OID:  colDef.TypeOID,
				Name: colDef.TypeName,
			}

		// Unchanged TOAST
		case 'u':
			row[colDef.Name] = d.toastPlaceholder
			types[colDef.Name] = ColumnTypeMetadata{
				OID:    colDef.TypeOID,
				Name:   colDef.TypeName,
				Format: "toast_placeholder",
			}

		// (typoutput)
		case 't':
			types[colDef.Name] = ColumnTypeMetadata{
				OID:  colDef.TypeOID,
				Name: colDef.TypeName,
			}
			val, err := decodeTextValue(col.Data, colDef.TypeOID)
			if err != nil {
				slog.Warn("decode text value failed, preserving as string",
					"table", rel.Schema+"."+rel.Name,
					"column", colDef.Name,
					"type_oid", colDef.TypeOID,
					"error", err)
				row[colDef.Name] = string(col.Data)
			} else {
				row[colDef.Name] = val
			}

		case 'b':
			return nil, nil, fmt.Errorf(
				"binary tuple data is not supported for %s.%s.%s (oid=%d)",
				rel.Schema, rel.Name, colDef.Name, colDef.TypeOID,
			)

		default:
			return nil, nil, fmt.Errorf(
				"unknown tuple data type %q for %s.%s.%s (oid=%d)",
				col.DataType, rel.Schema, rel.Name, colDef.Name, colDef.TypeOID,
			)
		}
	}

	return row, types, nil
}

// ExtractPK extracts primary key values from a decoded row using the relation's column defs.
func (d *Decoder) ExtractPK(relOID uint32, row map[string]any) (map[string]any, error) {
	rel, ok := d.relations.Get(relOID)
	if !ok {
		return nil, fmt.Errorf("unknown relation OID %d", relOID)
	}

	pk := make(map[string]any)
	for _, col := range rel.Columns {
		if col.IsPrimaryKey {
			val, present := row[col.Name]
			if !present {
				return nil, fmt.Errorf("missing primary key column %s.%s.%s in row", rel.Schema, rel.Name, col.Name)
			}
			if val == nil {
				return nil, fmt.Errorf("primary key column %s.%s.%s is nil", rel.Schema, rel.Name, col.Name)
			}
			pk[col.Name] = val
		}
	}
	return pk, nil
}

// PKChanged returns true if the primary key values differ between before and after.
func (d *Decoder) PKChanged(relOID uint32, before, after map[string]any) (bool, error) {
	rel, ok := d.relations.Get(relOID)
	if !ok {
		return false, fmt.Errorf("unknown relation OID %d", relOID)
	}

	for _, col := range rel.Columns {
		if !col.IsPrimaryKey {
			continue
		}
		bv, bOk := before[col.Name]
		if !bOk {
			return false, fmt.Errorf("missing primary key column %s.%s.%s in before row", rel.Schema, rel.Name, col.Name)
		}
		if bv == nil {
			return false, fmt.Errorf("primary key column %s.%s.%s is nil in before row", rel.Schema, rel.Name, col.Name)
		}

		av, aOk := after[col.Name]
		if !aOk {
			return false, fmt.Errorf("missing primary key column %s.%s.%s in after row", rel.Schema, rel.Name, col.Name)
		}
		if av == nil {
			return false, fmt.Errorf("primary key column %s.%s.%s is nil in after row", rel.Schema, rel.Name, col.Name)
		}
		if !pkValuesEqual(bv, av) {
			return true, nil
		}
	}
	return false, nil
}

// pkValuesEqual compares two PK values for equality.
func pkValuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case uint64:
		if bv, ok := b.(uint64); ok {
			return av == bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	}
	// falling back to string comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// decodeTextValue converts a typoutput format value into a JSON-safe Go type.
// decisions for JSON safety:
//   - int2/int4: returned as int64 (fits in float64 without precision loss)
//   - int8: returned as int64 if |value| ≤ 2^53, otherwise as string to prevent
//     float64 precision loss during JSON round-trip
//   - float4/float8: NaN/Infinity returned as string (not valid JSON numbers)
//   - numeric/money: returned as string (exact decimal, no float precision loss)
//   - json/jsonb: returned as json.RawMessage
//   - all temporal/network/other types: returned as string
func decodeTextValue(data []byte, typeOID uint32) (any, error) {
	s := string(data)

	switch typeOID {
	case oidBool:
		return s == "t" || s == "true" || s == "TRUE", nil

	case oidInt2:
		return strconv.ParseInt(s, 10, 16)
	case oidInt4:
		return strconv.ParseInt(s, 10, 32)
	case oidInt8:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		// values outside the float64 safe integer range (±2^53) would lose
		// precision when json.Unmarshal decodes them into float64.
		// we are storing them as strings; the reader will use column_types.oid to know
		// this is an int8 and can parse the string with strconv.ParseInt.
		if v > maxSafeJSONInt || v < -maxSafeJSONInt {
			return s, nil
		}
		return v, nil

	case oidFloat4:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		f := float32(v)
		// "NaN", "Infinity", "-Infinity" as string
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return s, nil
		}
		return f, nil

	case oidFloat8:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return s, nil
		}
		return v, nil

	case oidNumeric, oidMoney:
		return s, nil

	case oidText, oidVarChar, oidBPChar, oidChar, oidName, oidXML:
		return s, nil

	case oidJSON, oidJSONB:
		return json.RawMessage(data), nil

	case oidUUID:
		return s, nil

	case oidByteA:
		if strings.HasPrefix(s, `\x`) {
			return s, nil
		}
		return s, nil

	case oidDate:
		return s, nil
	case oidTime, oidTimeTZ:
		return s, nil
	case oidTimestamp:
		return s, nil
	case oidTimestampTZ:
		return s, nil
	case oidInterval:
		return s, nil

	case oidInet, oidMacAddr, oidMacAddr8:
		return s, nil

	case oidPgLSN:
		return s, nil

	case oidOID, oidXID, oidCID:
		return strconv.ParseUint(s, 10, 32)

	default:
		return s, nil
	}
}
