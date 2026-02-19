package postgres

type ColumnTransform struct {
	// Columns schema.table.column
	Columns string `json:"columns" yaml:"columns" toml:"columns"`
	// mask, hash, truncate
	Type          string `json:"type" yaml:"type" toml:"type"`
	MaskLength    int    `json:"maskLength,omitempty" yaml:"maskLength,omitempty" toml:"maskLength,omitempty"`
	HashAlgorithm string `json:"hashAlgorithm,omitempty" yaml:"hashAlgorithm,omitempty" toml:"hashAlgorithm,omitempty"`
	HashSalt      string `json:"hashSalt,omitempty" yaml:"hashSalt,omitempty" toml:"hashSalt,omitempty"`
	TruncateChar  int    `json:"truncateChar,omitempty" yaml:"truncateChar,omitempty" toml:"truncateChar,omitempty"`
}

var validTransformTypes = map[string]bool{
	"mask":     true,
	"hash":     true,
	"truncate": true,
}
