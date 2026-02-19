package postgres

import (
	"errors"
	"fmt"
	"os"
	"regexp"
)

// Config embeds all the configuration needed for the postgres connector to work.
type Config struct {
	// This should be unique for each because this is what we will use
	// in s3/azure prefix
	Name       string           `json:"name" toml:"name" yaml:"name"`
	Connection ConnectionConfig `json:"connection" toml:"connection" yaml:"connection"`

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

// ConnectionConfig holds PostgreSQL connection Parameters
type ConnectionConfig struct {
	Host     string `json:"host" yaml:"host" toml:"host"`
	Port     int    `json:"port" yaml:"port" toml:"port"`
	User     string `json:"user" yaml:"user" toml:"user"`
	Password string `json:"password,omitempty" yaml:"password,omitempty" toml:"password,omitempty"`
	// We will use this if user configure through ENV like "env:PG_PASSWORD"
	PasswordRef    string `json:"password_ref,omitempty" yaml:"password_ref,omitempty" toml:"password_ref,omitempty"`
	Database       string `json:"database" yaml:"database" toml:"database"`
	SSLMode        string `json:"ssl_mode,omitempty" yaml:"ssl_mode,omitempty" toml:"ssl_mode,omitempty"`
	ConnectTimeout string `json:"connect_timeout,omitempty" yaml:"connect_timeout,omitempty" toml:"connect_timeout,omitempty"`
}

func (c ConnectionConfig) DSN() string {
	port := c.Port
	if port == 0 {
		port = 5432
	}
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	pw := c.Password
	if pw == "" && c.PasswordRef != "" {
		pw = resolveENVRef(c.PasswordRef)
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%s",
		c.Host, port, c.Database, c.User, pw, sslMode, c.connectTimeout(),
	)
}

// ReplicationDSN returns a connection string for the replication protocol.
func (c ConnectionConfig) ReplicationDSN() string {
	port := c.Port
	if port == 0 {
		port = 5432
	}
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	pw := c.Password
	if pw == "" && c.PasswordRef != "" {
		pw = resolveENVRef(c.PasswordRef)
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s replication=database connect_timeout=%s",
		c.Host, port, c.Database, c.User, pw, sslMode, c.connectTimeout(),
	)
}

func (c ConnectionConfig) connectTimeout() string {
	if c.ConnectTimeout != "" {
		return c.ConnectTimeout
	}
	return "10"
}

func resolveENVRef(ref string) string {
	if len(ref) > 4 && ref[:4] == "env:" {
		return os.Getenv(ref[4:])
	}
	return ref
}
