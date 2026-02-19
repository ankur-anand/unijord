package postgres

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"hash"
	"log/slog"
	"strings"
	"time"
)

type EnvelopeBuilder struct {
	connectorName string
	database      string
	transforms    []ColumnTransform
}

func NewEnvelopeBuilder(cfg Config) *EnvelopeBuilder {
	return &EnvelopeBuilder{
		connectorName: cfg.Name,
		database:      cfg.Connection.Database,
		transforms:    cfg.Transforms,
	}
}

// BuildInsert creates a CDC envelope for an INSERT.
func (eb *EnvelopeBuilder) BuildInsert(
	rel *RelationInfo,
	after map[string]any,
	colTypes map[string]ColumnTypeMetadata,
	lsn uint64, txID uint64, commitTime time.Time,
	isSnapshot bool,
) Envelope {
	op := OpCreate
	snapshotFlag := "false"
	if isSnapshot {
		op = OpRead
		snapshotFlag = "true"
	}

	after = eb.applyTransforms(rel.Schema, rel.Name, after)
	now := time.Now()

	return Envelope{
		Payload: EnvelopePayload{
			Before: nil,
			After:  after,
			Source: SourceMetadata{
				Version:       ConnectorVersion001,
				ConnectorName: eb.connectorName,
				TableName:     rel.Name,
				RelSchema:     rel.Schema,
				DBName:        eb.database,
				LSN:           lsn,
				TxID:          txID,
				CommitTimeUS:  commitTime.UnixMicro(),
				IsSnapshot:    snapshotFlag,
			},
			Op:          op,
			TsUSec:      now.UnixMicro(),
			ColumnTypes: colTypes,
		},
	}
}

// BuildUpdate creates a CDC envelope for an UPDATE.
func (eb *EnvelopeBuilder) BuildUpdate(
	rel *RelationInfo,
	before, after map[string]any,
	colTypes map[string]ColumnTypeMetadata,
	lsn uint64, txID uint64, commitTime time.Time,
) Envelope {
	before = eb.applyTransforms(rel.Schema, rel.Name, before)
	after = eb.applyTransforms(rel.Schema, rel.Name, after)
	now := time.Now()

	return Envelope{
		Payload: EnvelopePayload{
			Before: before,
			After:  after,
			Source: SourceMetadata{
				Version:       ConnectorVersion001,
				ConnectorName: eb.connectorName,
				TableName:     rel.Name,
				RelSchema:     rel.Schema,
				DBName:        eb.database,
				LSN:           lsn,
				TxID:          txID,
				CommitTimeUS:  commitTime.UnixMicro(),
				IsSnapshot:    "false",
			},
			Op:          OpUpdate,
			TsUSec:      now.UnixMicro(),
			ColumnTypes: colTypes,
		},
	}
}

// BuildDelete creates a CDC envelope for a DELETE.
func (eb *EnvelopeBuilder) BuildDelete(
	rel *RelationInfo,
	before map[string]any,
	colTypes map[string]ColumnTypeMetadata,
	lsn uint64, txID uint64, commitTime time.Time,
) Envelope {
	before = eb.applyTransforms(rel.Schema, rel.Name, before)
	now := time.Now()

	return Envelope{
		Payload: EnvelopePayload{
			Before: before,
			After:  nil,
			Source: SourceMetadata{
				Version:       ConnectorVersion001,
				ConnectorName: eb.connectorName,
				TableName:     rel.Name,
				RelSchema:     rel.Schema,
				DBName:        eb.database,
				LSN:           lsn,
				TxID:          txID,
				CommitTimeUS:  commitTime.UnixMicro(),
				IsSnapshot:    "false",
			},
			Op:          OpDelete,
			TsUSec:      now.UnixMicro(),
			ColumnTypes: colTypes,
		},
	}
}

// BuildTruncate creates a CDC envelope for a TRUNCATE.
func (eb *EnvelopeBuilder) BuildTruncate(
	rel *RelationInfo,
	lsn uint64, txID uint64, commitTime time.Time,
) Envelope {
	now := time.Now()

	return Envelope{
		Payload: EnvelopePayload{
			Before: nil,
			After:  nil,
			Source: SourceMetadata{
				Version:       ConnectorVersion001,
				ConnectorName: eb.connectorName,
				TableName:     rel.Name,
				RelSchema:     rel.Schema,
				DBName:        eb.database,
				LSN:           lsn,
				TxID:          txID,
				CommitTimeUS:  commitTime.UnixMicro(),
				IsSnapshot:    "false",
			},
			Op:     OpTruncate,
			TsUSec: now.UnixMicro(),
		},
	}
}

func MarshalEnvelope(env Envelope) ([]byte, error) {
	return json.Marshal(env)
}

func (eb *EnvelopeBuilder) applyTransforms(schema, table string, row map[string]any) map[string]any {
	if row == nil || len(eb.transforms) == 0 {
		return row
	}

	for _, t := range eb.transforms {
		for colName, val := range row {
			fqn := schema + "." + table + "." + colName
			if !matchesAny(fqn, []string{t.Columns}) {
				continue
			}
			switch t.Type {
			case "mask":
				length := t.MaskLength
				if length <= 0 {
					length = 5
				}
				row[colName] = strings.Repeat("*", length)
			case "hash":
				row[colName] = hashValue(val, t.HashAlgorithm, t.HashSalt)
			case "truncate":
				if s, ok := val.(string); ok && t.TruncateChar > 0 && len(s) > t.TruncateChar {
					row[colName] = s[:t.TruncateChar]
				}
			default:
				slog.Error("unknown transform type, skipping (possible PII leak)",
					"type", t.Type,
					"column", fqn)
			}
		}
	}
	return row
}

func hashValue(val any, algo, salt string) string {
	s := fmt.Sprintf("%v", val)

	var newHash func() hash.Hash
	switch strings.ToLower(algo) {
	case "sha512":
		newHash = sha512.New
	case "sha256", "":
		newHash = sha256.New
	default:
		slog.Warn("isle-cdc: unknown hash algorithm, falling back to sha256",
			"algorithm", algo)
		newHash = sha256.New
	}

	if salt != "" {
		mac := hmac.New(newHash, []byte(salt))
		mac.Write([]byte(s))
		return fmt.Sprintf("%x", mac.Sum(nil))
	}

	h := newHash()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}
