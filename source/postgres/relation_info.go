package postgres

import "sync"

// RelationCache will hold map of OID Relation and their Column Definitions
type RelationCache struct {
	mu        sync.RWMutex
	relations map[uint32]*RelationInfo
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
