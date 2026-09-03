package postgres

import _ "embed"

const SchemaVersion = 1

//go:embed migrations/001_foundation.sql
var foundationSQL string
