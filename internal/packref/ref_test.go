package packref

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/namespaceid"
	"github.com/ankur-anand/unijord/internal/ujpk"
)

func TestValidate(t *testing.T) {
	valid := Ref{ID: [16]byte{1}, Key: "packs/one.ujpk", FormatVersion: ujpk.Version,
		NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: 7,
		FirstChunkSequence: 1, LastChunkSequence: 2, RecordCount: 3, TimelineCount: 2,
		SizeBytes: 512, SHA256: sha256.Sum256([]byte("pack"))}
	if err := Validate(valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.LastChunkSequence = 0
	if err := Validate(invalid); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("Validate() error=%v", err)
	}
	if !MatchesIdentity(valid, ujpk.Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: 7}) ||
		MatchesIdentity(valid, ujpk.Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-b")), Shard: 7}) {
		t.Fatal("pack identity match accepted the wrong namespace")
	}
}
