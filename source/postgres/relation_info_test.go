package postgres

import (
	"strings"
	"testing"
)

func TestRelationCacheUpdateRejectsExcludedPrimaryKey(t *testing.T) {
	rc := NewRelationCache()
	rc.SetFilter(FilterConfig{
		ColumnExclude: []string{`^public\.users\.id$`},
	})

	err := rc.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "email", IsPrimaryKey: false},
		},
	})
	if err == nil {
		t.Fatal("Update() expected error when PK column is excluded, got nil")
	}
	if !strings.Contains(err.Error(), "column_exclude matches primary key column public.users.id") {
		t.Fatalf("Update() error = %q, want PK exclusion message", err.Error())
	}

	if _, ok := rc.Get(1); ok {
		t.Fatal("relation was cached despite invalid PK exclusion")
	}
}

func TestRelationCacheUpdateAllowsNonPrimaryKeyExclusion(t *testing.T) {
	rc := NewRelationCache()
	rc.SetFilter(FilterConfig{
		ColumnExclude: []string{`^public\.users\.email$`},
	})

	err := rc.Update(1, &RelationInfo{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "email", IsPrimaryKey: false},
		},
	})
	if err != nil {
		t.Fatalf("Update() unexpected error: %v", err)
	}

	if _, ok := rc.Get(1); !ok {
		t.Fatal("expected relation to be cached")
	}
}
