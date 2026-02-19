package postgres

import "fmt"

// Table represent a captured postgreSQL table.
type Table struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	OID    uint32 `json:"oid"`

	Columns []TableColumn `json:"columns"`
	// ReplicationIdentity is the replication mode for a table.
	// https://www.postgresql.org/docs/current/logical-replication-publication.html#LOGICAL-REPLICATION-PUBLICATION-REPLICA-IDENTITY
	ReplicationIdentity string `json:"replication_identity"`
}

type TableColumn struct {
	Name         string `json:"name"`
	TypeOID      uint32 `json:"oid"`
	TypeName     string `json:"type"`
	IsPrimaryKey bool   `json:"is_pkey"`
	IsToastable  bool   `json:"toastable"`
}

func (t Table) FullName() string {
	return t.Schema + "." + t.Name
}

func (t Table) QuotedFullName() string {
	return fmt.Sprintf(`"%s".""%s"`, t.Schema, t.Name)
}

func (t Table) PrimaryKeyColumns() []string {
	var pks []string
	for _, col := range t.Columns {
		if col.IsPrimaryKey {
			pks = append(pks, col.Name)
		}
	}
	return pks
}

func (t Table) HasToastColumn() bool {
	for _, col := range t.Columns {
		if col.IsToastable {
			return true
		}
	}
	return false
}

func (t Table) ToastColumnsNames() []string {
	var names []string
	for _, col := range t.Columns {
		if col.IsToastable {
			names = append(names, col.Name)
		}
	}
	return names
}
