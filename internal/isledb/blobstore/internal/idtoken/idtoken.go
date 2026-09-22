// Package idtoken encodes provider-owned run identity tokens. It lives under
// blobstore/internal so that only provider leaves can construct or parse a
// token; every other caller treats the token as an opaque string.
package idtoken

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const version = "v1"

var ErrMalformed = errors.New("idtoken: malformed identity token")

// Encode renders "<provider>.v1.<base64url(JSON(fields))>". fields must be a
// struct so member order, and therefore the token, is deterministic.
func Encode(provider string, fields any) (string, error) {
	if provider == "" || strings.ContainsAny(provider, ". ") {
		return "", ErrMalformed
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		return "", errors.Join(ErrMalformed, err)
	}
	return provider + "." + version + "." + base64.RawURLEncoding.EncodeToString(payload), nil
}

// Decode accepts only this provider, this version, known fields, and no
// trailing data. It then requires the canonical re-encoding to match so two
// spellings can never name one identity.
func Decode(provider, token string, fields any) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != provider || parts[1] != version || parts[2] == "" {
		return ErrMalformed
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return errors.Join(ErrMalformed, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(fields); err != nil {
		return errors.Join(ErrMalformed, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ErrMalformed
	}
	canonical, err := Encode(provider, fields)
	if err != nil || canonical != token {
		return ErrMalformed
	}
	return nil
}
