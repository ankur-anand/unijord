package blobstore

import (
	"encoding/hex"
	"errors"
	"strings"
)

var ErrInvalidRunPath = errors.New("invalid canonical run path")

// ParseRunID accepts exactly one lowercase, nonzero, 16-byte hexadecimal ID.
func ParseRunID(s string) (id [16]byte, err error) {
	if len(s) != 32 || s != strings.ToLower(s) {
		return id, ErrInvalidRunPath
	}
	n, err := hex.Decode(id[:], []byte(s))
	if err != nil || n != len(id) || id == [16]byte{} {
		return [16]byte{}, ErrInvalidRunPath
	}
	return id, nil
}

// RunPath derives placement only from the validated run identity. Fanout uses
// the existing CRC32C/12-bit object distribution over the lowercase hex ID.
func (s *Store) RunPath(id [16]byte) (string, error) {
	if id == [16]byte{} || !canonicalRunPrefix(s.prefix) {
		return "", ErrInvalidRunPath
	}
	text := hex.EncodeToString(id[:])
	key := "runs/" + RunBucket(text) + "/" + text + ".ujrn"
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}
	return key, nil
}

// ParseRunPath rejects alternate spellings instead of cleaning caller input.
func (s *Store) ParseRunPath(key string) ([16]byte, error) {
	if len(key) < 37 || !strings.HasSuffix(key, ".ujrn") {
		return [16]byte{}, ErrInvalidRunPath
	}
	id, err := ParseRunID(key[len(key)-37 : len(key)-5])
	if err != nil {
		return [16]byte{}, err
	}
	want, err := s.RunPath(id)
	if err != nil || key != want {
		return [16]byte{}, ErrInvalidRunPath
	}
	return id, nil
}

func canonicalRunPrefix(prefix string) bool {
	if prefix == "" {
		return true
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
		for _, c := range part {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
				return false
			}
		}
	}
	return true
}
