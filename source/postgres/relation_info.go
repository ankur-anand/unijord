package postgres

import (
	"fmt"
	"sync"
)

// RelationCache will hold map of OID Relation and their Column Definitions
type RelationCache struct {
	mu        sync.RWMutex
	relations map[uint32]*RelationInfo
	filter    FilterConfig
}

type RelationInfo struct {
	OID     uint32
	Schema  string
	Name    string
	Columns []Column
}

func NewRelationCache() *RelationCache {
	return &RelationCache{
		relations: make(map[uint32]*RelationInfo),
	}
}

// SetFilter configures table/column filters used for relation validation.
func (rc *RelationCache) SetFilter(filter FilterConfig) {
	rc.mu.Lock()
	rc.filter = filter
	rc.mu.Unlock()
}

// Update upserts relation metadata and rejects configs that exclude PK columns.
func (rc *RelationCache) Update(oid uint32, info *RelationInfo) error {
	if err := rc.validatePKNotExcluded(info); err != nil {
		return err
	}

	rc.mu.Lock()
	rc.relations[oid] = info
	rc.mu.Unlock()
	return nil
}

func (rc *RelationCache) Get(oid uint32) (*RelationInfo, bool) {
	rc.mu.RLock()
	r, ok := rc.relations[oid]
	rc.mu.RUnlock()
	return r, ok
}

func (rc *RelationCache) validatePKNotExcluded(info *RelationInfo) error {
	rc.mu.RLock()
	filter := rc.filter
	rc.mu.RUnlock()

	for _, col := range info.Columns {
		if !col.IsPrimaryKey {
			continue
		}
		if filter.ColumnExcluded(info.Schema, info.Name, col.Name) {
			return fmt.Errorf(
				"column_exclude matches primary key column %s.%s.%s",
				info.Schema, info.Name, col.Name,
			)
		}
	}
	return nil
}
