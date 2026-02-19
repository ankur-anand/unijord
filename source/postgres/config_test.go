package postgres

import (
	"encoding/json"
	"strings"
	"testing"

	toml "github.com/pelletier/go-toml/v2"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "empty transforms",
			config:  Config{},
			wantErr: "",
		},
		{
			name: "valid transform",
			config: Config{
				Transforms: []ColumnTransform{
					{
						Columns: "public\\.users\\.(email|name)",
						Type:    "mask",
					},
				},
			},
			wantErr: "",
		},
		{
			name: "empty columns",
			config: Config{
				Transforms: []ColumnTransform{
					{
						Columns: "",
						Type:    "mask",
					},
				},
			},
			wantErr: "columns is empty for transform",
		},
		{
			name: "invalid columns regex",
			config: Config{
				Transforms: []ColumnTransform{
					{
						Columns: "[",
						Type:    "mask",
					},
				},
			},
			wantErr: "columns is invalid for transform",
		},
		{
			name: "invalid transform type",
			config: Config{
				Transforms: []ColumnTransform{
					{
						Columns: "public\\.users\\.email",
						Type:    "encrypt",
					},
				},
			},
			wantErr: "invalid transform type: encrypt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" && err != nil {
				t.Fatalf("Validate() unexpected error: %v", err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Validate() expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Validate() error = %q, want contains %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

func TestConfigMarshalJSONOmitsEmptyTransforms(t *testing.T) {
	cfg := Config{}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	if string(data) != "{}" {
		t.Fatalf("json.Marshal() = %s, want {}", string(data))
	}
}

func TestConfigMarshalTOMLOmitsEmptyTransforms(t *testing.T) {
	cfg := Config{}
	data, err := toml.Marshal(cfg)
	if err != nil {
		t.Fatalf("toml.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := toml.Unmarshal(data, &got); err != nil {
		t.Fatalf("toml.Unmarshal() error: %v", err)
	}

	if _, ok := got["transforms"]; ok {
		t.Fatalf("toml.Marshal() should omit transforms, got %s", string(data))
	}
}
