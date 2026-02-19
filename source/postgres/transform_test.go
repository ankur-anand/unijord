package postgres

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	toml "github.com/pelletier/go-toml/v2"
)

func TestColumnTransformMarshalJSONOmitsEmptyOptionalFields(t *testing.T) {
	transform := ColumnTransform{
		Columns: "public.users.email",
		Type:    "mask",
	}

	data, err := json.Marshal(transform)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if _, ok := got["columns"]; !ok {
		t.Fatalf("expected columns in JSON output, got %v", got)
	}
	if _, ok := got["type"]; !ok {
		t.Fatalf("expected type in JSON output, got %v", got)
	}

	optionalKeys := []string{"maskLength", "hashAlgorithm", "hashSalt", "truncateChar"}
	for _, key := range optionalKeys {
		if _, ok := got[key]; ok {
			t.Fatalf("expected %q to be omitted from JSON output, got %v", key, got)
		}
	}
}

func TestColumnTransformMarshalJSONIncludesOptionalFieldsWhenSet(t *testing.T) {
	transform := ColumnTransform{
		Columns:       "public.users.email",
		Type:          "hash",
		MaskLength:    4,
		HashAlgorithm: "sha256",
		HashSalt:      "salt",
		TruncateChar:  3,
	}

	data, err := json.Marshal(transform)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	for _, key := range []string{"maskLength", "hashAlgorithm", "hashSalt", "truncateChar"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("expected %q in JSON output, got %v", key, got)
		}
	}
}

func TestColumnTransformMarshalTOMLOmitsEmptyOptionalFields(t *testing.T) {
	transform := ColumnTransform{
		Columns: "public.users.email",
		Type:    "mask",
	}

	data, err := toml.Marshal(transform)
	if err != nil {
		t.Fatalf("toml.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := toml.Unmarshal(data, &got); err != nil {
		t.Fatalf("toml.Unmarshal() error: %v", err)
	}

	if _, ok := got["columns"]; !ok {
		t.Fatalf("expected columns in TOML output, got %v", got)
	}
	if _, ok := got["type"]; !ok {
		t.Fatalf("expected type in TOML output, got %v", got)
	}

	optionalKeys := []string{"maskLength", "hashAlgorithm", "hashSalt", "truncateChar"}
	for _, key := range optionalKeys {
		if _, ok := got[key]; ok {
			t.Fatalf("expected %q to be omitted from TOML output, got %v", key, got)
		}
	}
}

func TestColumnTransformMarshalTOMLIncludesOptionalFieldsWhenSet(t *testing.T) {
	transform := ColumnTransform{
		Columns:       "public.users.email",
		Type:          "hash",
		MaskLength:    4,
		HashAlgorithm: "sha256",
		HashSalt:      "salt",
		TruncateChar:  3,
	}

	data, err := toml.Marshal(transform)
	if err != nil {
		t.Fatalf("toml.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := toml.Unmarshal(data, &got); err != nil {
		t.Fatalf("toml.Unmarshal() error: %v", err)
	}

	for _, key := range []string{"maskLength", "hashAlgorithm", "hashSalt", "truncateChar"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("expected %q in TOML output, got %v", key, got)
		}
	}
}

func TestColumnTransformOptionalFieldsHaveOmitemptyTags(t *testing.T) {
	tp := reflect.TypeOf(ColumnTransform{})

	fields := []string{"MaskLength", "HashAlgorithm", "HashSalt", "TruncateChar"}
	for _, fieldName := range fields {
		field, ok := tp.FieldByName(fieldName)
		if !ok {
			t.Fatalf("field %q not found", fieldName)
		}

		for _, tagName := range []string{"json", "yaml", "toml"} {
			tagValue := field.Tag.Get(tagName)
			if !strings.Contains(tagValue, "omitempty") {
				t.Fatalf("expected %s tag on %s to contain omitempty, got %q", tagName, fieldName, tagValue)
			}
		}
	}
}
