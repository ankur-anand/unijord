package postgres

import (
	"strings"
	"testing"
)

func TestDecoderDecodeTupleText(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{
				Name:     "id",
				TypeOID:  oidInt4,
				TypeName: "int4",
			},
		},
	})

	decoder := NewDecoder(relations, Config{})
	row, types, err := decoder.DecodeTuple(1, []TupleDataColumn{
		{
			DataType: 't',
			Data:     []byte("42"),
		},
	})
	if err != nil {
		t.Fatalf("DecodeTuple() error: %v", err)
	}

	got, ok := row["id"].(int64)
	if !ok {
		t.Fatalf("row[id] has unexpected type %T with value %v", row["id"], row["id"])
	}
	if got != 42 {
		t.Fatalf("row[id] = %d, want 42", got)
	}

	typeMeta, ok := types["id"]
	if !ok {
		t.Fatalf("types[id] missing")
	}
	if typeMeta.OID != oidInt4 || typeMeta.Name != "int4" {
		t.Fatalf("types[id] = %+v, want OID=%d Name=int4", typeMeta, oidInt4)
	}
}

func TestDecoderDecodeTupleBinaryUnsupported(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{
				Name:     "id",
				TypeOID:  oidInt4,
				TypeName: "int4",
			},
		},
	})

	decoder := NewDecoder(relations, Config{})
	row, types, err := decoder.DecodeTuple(1, []TupleDataColumn{
		{
			DataType: 'b',
			Data:     []byte{0, 0, 0, 42},
		},
	})
	if err == nil {
		t.Fatal("DecodeTuple() expected error for binary tuple data, got nil")
	}
	if !strings.Contains(err.Error(), "binary tuple data is not supported") {
		t.Fatalf("DecodeTuple() error = %q, want contains %q", err.Error(), "binary tuple data is not supported")
	}
	if row != nil || types != nil {
		t.Fatalf("DecodeTuple() row/types = (%v, %v), want (nil, nil) on error", row, types)
	}
}

func TestDecoderDecodeTupleUnknownDataType(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{
				Name:     "id",
				TypeOID:  oidInt4,
				TypeName: "int4",
			},
		},
	})

	decoder := NewDecoder(relations, Config{})
	_, _, err := decoder.DecodeTuple(1, []TupleDataColumn{
		{
			DataType: 'x',
			Data:     []byte("42"),
		},
	})
	if err == nil {
		t.Fatal("DecodeTuple() expected error for unknown tuple data type, got nil")
	}
	if !strings.Contains(err.Error(), "unknown tuple data type") {
		t.Fatalf("DecodeTuple() error = %q, want contains %q", err.Error(), "unknown tuple data type")
	}
}

func TestDecoderDecodeTupleNullIncludesTypeMetadata(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{
				Name:     "id",
				TypeOID:  oidInt4,
				TypeName: "int4",
			},
		},
	})

	decoder := NewDecoder(relations, Config{})
	row, types, err := decoder.DecodeTuple(1, []TupleDataColumn{
		{
			DataType: 'n',
		},
	})
	if err != nil {
		t.Fatalf("DecodeTuple() error: %v", err)
	}

	if val, ok := row["id"]; !ok || val != nil {
		t.Fatalf("row[id] = %v (present=%v), want nil present", val, ok)
	}

	meta, ok := types["id"]
	if !ok {
		t.Fatalf("types[id] missing for NULL value")
	}
	if meta.OID != oidInt4 || meta.Name != "int4" {
		t.Fatalf("types[id] = %+v, want OID=%d Name=int4", meta, oidInt4)
	}
}

func TestDecoderExtractPKMissingColumn(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
		},
	})

	decoder := NewDecoder(relations, Config{})
	_, err := decoder.ExtractPK(1, map[string]any{"name": "alice"})
	if err == nil {
		t.Fatal("ExtractPK() expected error for missing PK column, got nil")
	}
	if !strings.Contains(err.Error(), "missing primary key column public.users.id") {
		t.Fatalf("ExtractPK() error = %q, want missing PK message", err.Error())
	}
}

func TestDecoderExtractPKNilColumn(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
		},
	})

	decoder := NewDecoder(relations, Config{})
	_, err := decoder.ExtractPK(1, map[string]any{"id": nil})
	if err == nil {
		t.Fatal("ExtractPK() expected error for nil PK value, got nil")
	}
	if !strings.Contains(err.Error(), "primary key column public.users.id is nil") {
		t.Fatalf("ExtractPK() error = %q, want nil PK message", err.Error())
	}
}

func TestDecoderPKChangedErrorsWhenPKMissing(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
		},
	})

	decoder := NewDecoder(relations, Config{})
	_, err := decoder.PKChanged(1, map[string]any{}, map[string]any{"id": int64(1)})
	if err == nil {
		t.Fatal("PKChanged() expected error for missing PK in before row, got nil")
	}
	if !strings.Contains(err.Error(), "missing primary key column public.users.id in before row") {
		t.Fatalf("PKChanged() error = %q, want missing-before message", err.Error())
	}

	_, err = decoder.PKChanged(1, map[string]any{"id": int64(1)}, map[string]any{})
	if err == nil {
		t.Fatal("PKChanged() expected error for missing PK in after row, got nil")
	}
	if !strings.Contains(err.Error(), "missing primary key column public.users.id in after row") {
		t.Fatalf("PKChanged() error = %q, want missing-after message", err.Error())
	}
}

func TestDecoderPKChangedErrorsWhenPKNil(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
		},
	})

	decoder := NewDecoder(relations, Config{})
	_, err := decoder.PKChanged(1, map[string]any{"id": nil}, map[string]any{"id": int64(1)})
	if err == nil {
		t.Fatal("PKChanged() expected error for nil PK in before row, got nil")
	}
	if !strings.Contains(err.Error(), "primary key column public.users.id is nil in before row") {
		t.Fatalf("PKChanged() error = %q, want nil-before message", err.Error())
	}

	_, err = decoder.PKChanged(1, map[string]any{"id": int64(1)}, map[string]any{"id": nil})
	if err == nil {
		t.Fatal("PKChanged() expected error for nil PK in after row, got nil")
	}
	if !strings.Contains(err.Error(), "primary key column public.users.id is nil in after row") {
		t.Fatalf("PKChanged() error = %q, want nil-after message", err.Error())
	}
}

func TestDecoderPKChangedDetectsDifference(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
		},
	})

	decoder := NewDecoder(relations, Config{})
	changed, err := decoder.PKChanged(1, map[string]any{"id": int64(1)}, map[string]any{"id": int64(2)})
	if err != nil {
		t.Fatalf("PKChanged() error: %v", err)
	}
	if !changed {
		t.Fatal("PKChanged() = false, want true")
	}
}

func TestDecoderPKChangedNoDifference(t *testing.T) {
	relations := NewRelationCache()
	relations.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
		},
	})

	decoder := NewDecoder(relations, Config{})
	changed, err := decoder.PKChanged(1, map[string]any{"id": int64(1)}, map[string]any{"id": int64(1)})
	if err != nil {
		t.Fatalf("PKChanged() error: %v", err)
	}
	if changed {
		t.Fatal("PKChanged() = true, want false")
	}
}
