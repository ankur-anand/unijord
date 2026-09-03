// Package packref defines the catalog-independent publication reference for
// one immutable UJPK object.
package packref

import (
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/internal/ujpk"
)

var ErrInvalidRef = errors.New("packref: invalid reference")

type Ref struct {
	ID                 [16]byte
	Key                string
	FormatVersion      uint16
	NamespaceHash      [32]byte
	Shard              uint32
	FirstChunkSequence uint64
	LastChunkSequence  uint64
	RecordCount        uint32
	TimelineCount      uint32
	SizeBytes          uint64
	SHA256             [32]byte
}

func Validate(ref Ref) error {
	if ref.ID == ([16]byte{}) || ref.Key == "" || ref.FormatVersion != ujpk.Version {
		return fmt.Errorf("%w: incomplete identity or format=%d", ErrInvalidRef, ref.FormatVersion)
	}
	if ref.NamespaceHash == ([32]byte{}) {
		return fmt.Errorf("%w: zero namespace hash", ErrInvalidRef)
	}
	if ref.LastChunkSequence < ref.FirstChunkSequence {
		return fmt.Errorf("%w: chunk range=[%d,%d]", ErrInvalidRef, ref.FirstChunkSequence, ref.LastChunkSequence)
	}
	if ref.RecordCount == 0 || ref.TimelineCount == 0 || ref.TimelineCount > ref.RecordCount {
		return fmt.Errorf("%w: records=%d timelines=%d", ErrInvalidRef, ref.RecordCount, ref.TimelineCount)
	}
	if ref.SizeBytes == 0 || ref.SHA256 == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete object identity", ErrInvalidRef)
	}
	return nil
}

func Same(a, b Ref) bool { return a == b }

func MatchesIdentity(ref Ref, identity ujpk.Identity) bool {
	return ref.NamespaceHash == identity.NamespaceHash && ref.Shard == identity.Shard
}
