package postgres

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestEnvelopeBuilderBuildInsertSourceMetadata(t *testing.T) {
	cfg := Config{
		Name: "connector-a",
		Connection: ConnectionConfig{
			Database: "appdb",
		},
	}
	eb := NewEnvelopeBuilder(cfg)
	rel := &RelationInfo{Schema: "public", Name: "users"}
	commitTime := time.Unix(1700000000, 0).UTC()

	envelope := eb.BuildInsert(rel, map[string]any{"id": 1}, nil, 42, 7, commitTime, false)

	want := SourceMetadata{
		Version:       ConnectorVersion001,
		ConnectorName: "connector-a",
		TableName:     "users",
		RelSchema:     "public",
		DBName:        "appdb",
		LSN:           42,
		TxID:          7,
		CommitTimeUS:  commitTime.UnixMicro(),
		IsSnapshot:    "false",
	}

	if envelope.Payload.Source != want {
		t.Fatalf("source = %+v, want %+v", envelope.Payload.Source, want)
	}

	if envelope.Payload.Op != OpCreate {
		t.Fatalf("op = %q, want %q", envelope.Payload.Op, OpCreate)
	}
	if envelope.Payload.TsUSec <= 0 {
		t.Fatalf("ts_u_sec = %d, want > 0", envelope.Payload.TsUSec)
	}
}

func TestEnvelopeBuilderBuildInsertSnapshotSourceMetadata(t *testing.T) {
	cfg := Config{
		Name: "connector-a",
		Connection: ConnectionConfig{
			Database: "appdb",
		},
	}
	eb := NewEnvelopeBuilder(cfg)
	rel := &RelationInfo{Schema: "public", Name: "users"}
	commitTime := time.Unix(1700000000, 0).UTC()

	envelope := eb.BuildInsert(rel, map[string]any{"id": 1}, nil, 101, 11, commitTime, true)

	if envelope.Payload.Source.IsSnapshot != "true" {
		t.Fatalf("is_snapshot = %q, want %q", envelope.Payload.Source.IsSnapshot, "true")
	}
	if envelope.Payload.Op != OpRead {
		t.Fatalf("op = %q, want %q", envelope.Payload.Op, OpRead)
	}
}

func TestEnvelopeBuilderBuildInsertJSONUsesMicrosecondFieldsOnly(t *testing.T) {
	cfg := Config{
		Name: "connector-a",
		Connection: ConnectionConfig{
			Database: "appdb",
		},
	}
	eb := NewEnvelopeBuilder(cfg)
	rel := &RelationInfo{Schema: "public", Name: "users"}
	commitTime := time.Unix(1700000000, 123000).UTC()

	envelope := eb.BuildInsert(rel, map[string]any{"id": 1}, nil, 42, 7, commitTime, false)
	data, err := MarshalEnvelope(envelope)
	if err != nil {
		t.Fatalf("MarshalEnvelope() error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}
	payload, ok := got["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload missing or wrong type in %v", got)
	}
	source, ok := payload["source"].(map[string]any)
	if !ok {
		t.Fatalf("source missing or wrong type in %v", payload)
	}

	if _, ok := payload["ts_u_sec"]; !ok {
		t.Fatalf("expected ts_u_sec in payload, got %v", payload)
	}
	if _, ok := payload["ts_m_sec"]; ok {
		t.Fatalf("did not expect ts_m_sec in payload, got %v", payload)
	}
	if _, ok := source["commit_time_us"]; !ok {
		t.Fatalf("expected commit_time_us in source, got %v", source)
	}
	if _, ok := source["commit_time_ms"]; ok {
		t.Fatalf("did not expect commit_time_ms in source, got %v", source)
	}
}

func TestEnvelopeBuilderBuildUpdate(t *testing.T) {
	cfg := Config{
		Name: "connector-a",
		Connection: ConnectionConfig{
			Database: "appdb",
		},
	}
	eb := NewEnvelopeBuilder(cfg)
	rel := &RelationInfo{Schema: "public", Name: "users"}
	commitTime := time.Unix(1700000000, 456000).UTC()
	before := map[string]any{"id": 1, "name": "old"}
	after := map[string]any{"id": 1, "name": "new"}
	colTypes := map[string]ColumnTypeMetadata{
		"id": {
			OID:    23,
			Name:   "int4",
			Format: "text",
		},
	}

	envelope := eb.BuildUpdate(rel, before, after, colTypes, 200, 21, commitTime)

	wantSource := SourceMetadata{
		Version:       ConnectorVersion001,
		ConnectorName: "connector-a",
		TableName:     "users",
		RelSchema:     "public",
		DBName:        "appdb",
		LSN:           200,
		TxID:          21,
		CommitTimeUS:  commitTime.UnixMicro(),
		IsSnapshot:    "false",
	}

	if envelope.Payload.Source != wantSource {
		t.Fatalf("source = %+v, want %+v", envelope.Payload.Source, wantSource)
	}
	if envelope.Payload.Op != OpUpdate {
		t.Fatalf("op = %q, want %q", envelope.Payload.Op, OpUpdate)
	}
	if envelope.Payload.TsUSec <= 0 {
		t.Fatalf("ts_u_sec = %d, want > 0", envelope.Payload.TsUSec)
	}
	if !reflect.DeepEqual(envelope.Payload.Before, before) {
		t.Fatalf("before = %+v, want %+v", envelope.Payload.Before, before)
	}
	if !reflect.DeepEqual(envelope.Payload.After, after) {
		t.Fatalf("after = %+v, want %+v", envelope.Payload.After, after)
	}
	if !reflect.DeepEqual(envelope.Payload.ColumnTypes, colTypes) {
		t.Fatalf("column_types = %+v, want %+v", envelope.Payload.ColumnTypes, colTypes)
	}
}

func TestEnvelopeBuilderBuildDelete(t *testing.T) {
	cfg := Config{
		Name: "connector-a",
		Connection: ConnectionConfig{
			Database: "appdb",
		},
	}
	eb := NewEnvelopeBuilder(cfg)
	rel := &RelationInfo{Schema: "public", Name: "users"}
	commitTime := time.Unix(1700000000, 789000).UTC()
	before := map[string]any{"id": 1, "name": "deleted"}
	colTypes := map[string]ColumnTypeMetadata{
		"id": {
			OID:    23,
			Name:   "int4",
			Format: "text",
		},
	}

	envelope := eb.BuildDelete(rel, before, colTypes, 300, 31, commitTime)

	wantSource := SourceMetadata{
		Version:       ConnectorVersion001,
		ConnectorName: "connector-a",
		TableName:     "users",
		RelSchema:     "public",
		DBName:        "appdb",
		LSN:           300,
		TxID:          31,
		CommitTimeUS:  commitTime.UnixMicro(),
		IsSnapshot:    "false",
	}

	if envelope.Payload.Source != wantSource {
		t.Fatalf("source = %+v, want %+v", envelope.Payload.Source, wantSource)
	}
	if envelope.Payload.Op != OpDelete {
		t.Fatalf("op = %q, want %q", envelope.Payload.Op, OpDelete)
	}
	if envelope.Payload.TsUSec <= 0 {
		t.Fatalf("ts_u_sec = %d, want > 0", envelope.Payload.TsUSec)
	}
	if !reflect.DeepEqual(envelope.Payload.Before, before) {
		t.Fatalf("before = %+v, want %+v", envelope.Payload.Before, before)
	}
	if envelope.Payload.After != nil {
		t.Fatalf("after = %+v, want nil", envelope.Payload.After)
	}
	if !reflect.DeepEqual(envelope.Payload.ColumnTypes, colTypes) {
		t.Fatalf("column_types = %+v, want %+v", envelope.Payload.ColumnTypes, colTypes)
	}
}

func TestEnvelopeBuilderBuildTruncate(t *testing.T) {
	cfg := Config{
		Name: "connector-a",
		Connection: ConnectionConfig{
			Database: "appdb",
		},
	}
	eb := NewEnvelopeBuilder(cfg)
	rel := &RelationInfo{Schema: "public", Name: "users"}
	commitTime := time.Unix(1700000001, 0).UTC()

	envelope := eb.BuildTruncate(rel, 400, 41, commitTime)

	wantSource := SourceMetadata{
		Version:       ConnectorVersion001,
		ConnectorName: "connector-a",
		TableName:     "users",
		RelSchema:     "public",
		DBName:        "appdb",
		LSN:           400,
		TxID:          41,
		CommitTimeUS:  commitTime.UnixMicro(),
		IsSnapshot:    "false",
	}

	if envelope.Payload.Source != wantSource {
		t.Fatalf("source = %+v, want %+v", envelope.Payload.Source, wantSource)
	}
	if envelope.Payload.Op != OpTruncate {
		t.Fatalf("op = %q, want %q", envelope.Payload.Op, OpTruncate)
	}
	if envelope.Payload.TsUSec <= 0 {
		t.Fatalf("ts_u_sec = %d, want > 0", envelope.Payload.TsUSec)
	}
	if envelope.Payload.Before != nil {
		t.Fatalf("before = %+v, want nil", envelope.Payload.Before)
	}
	if envelope.Payload.After != nil {
		t.Fatalf("after = %+v, want nil", envelope.Payload.After)
	}
	if envelope.Payload.ColumnTypes != nil {
		t.Fatalf("column_types = %+v, want nil", envelope.Payload.ColumnTypes)
	}
}
