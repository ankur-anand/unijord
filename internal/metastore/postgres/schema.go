package postgres

import _ "embed"

const SchemaVersion = 3

//go:embed migrations/001_foundation.sql
var foundationSQL string

//go:embed migrations/002_publication.sql
var publicationSQL string

//go:embed migrations/003_materializer_owner.sql
var materializerOwnerSQL string
