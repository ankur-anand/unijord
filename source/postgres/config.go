package postgres

import (
	"errors"
	"fmt"
	"regexp"
)

// Config embeds all the configuration needed for the postgres connector to work.
type Config struct {
	Transforms []ColumnTransform `json:"transforms,omitempty" yaml:"transforms,omitempty" toml:"transforms,omitempty"`
}

func (c *Config) Validate() error {
	for _, transform := range c.Transforms {
		if transform.Columns == "" {
			return errors.New("columns is empty for transform")
		}
		if _, err := regexp.Compile(transform.Columns); err != nil {
			return fmt.Errorf("columns is invalid for transform %w", err)
		}
		if !validTransformTypes[transform.Type] {
			return fmt.Errorf("invalid transform type: %s", transform.Type)
		}
	}
	return nil
}
