package postgres

// FilterConfig holds table/column include/exclude patterns.
type FilterConfig struct {
	SchemaInclude []string `json:"schema_include,omitempty"`
	SchemaExclude []string `json:"schema_exclude,omitempty"`
	TableInclude  []string `json:"table_include,omitempty"`
	TableExclude  []string `json:"table_exclude,omitempty"`
	ColumnExclude []string `json:"column_exclude,omitempty"`
}

// Matches returns true if the given schema.table passes the filter.
func (f FilterConfig) Matches(schema, table string) bool {
	fqn := schema + "." + table

	if len(f.SchemaInclude) > 0 && !matchesAny(schema, f.SchemaInclude) {
		return false
	}
	if matchesAny(schema, f.SchemaExclude) {
		return false
	}
	if len(f.TableInclude) > 0 && !matchesAny(fqn, f.TableInclude) {
		return false
	}
	if matchesAny(fqn, f.TableExclude) {
		return false
	}
	return true
}

// ColumnExcluded returns true if the given schema.table.column should be excluded.
func (f FilterConfig) ColumnExcluded(schema, table, column string) bool {
	fqn := schema + "." + table + "." + column
	return matchesAny(fqn, f.ColumnExclude)
}
