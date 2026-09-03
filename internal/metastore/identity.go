package metastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/ankur-anand/unijord/internal/namespaceid"
)

const (
	MaxNamespaceBytes = 1024
	MaxTimelineBytes  = 512
	MaxOwnerBytes     = 256
)

// Namespace is an owned immutable opaque namespace identity. CopyNamespace
// establishes ownership at an external boundary; TakeNamespace consumes an
// already-owned input. Bytes returns a read-only borrowed view.
type Namespace struct {
	identity *namespaceIdentity
}

type namespaceIdentity struct {
	exact  []byte
	digest [32]byte
}

// TimelineKey is an owned immutable timeline identity within a Namespace.
// Exact bytes are retained so a digest collision is detected, never accepted.
type TimelineKey struct {
	identity *timelineIdentity
}

type timelineIdentity struct {
	namespace Namespace
	exact     []byte
	digest    [32]byte
}

type ShardKey struct {
	Namespace Namespace
	Shard     uint32
}

// OwnerID is an immutable opaque runtime-owner identity. Go strings preserve
// arbitrary bytes and cannot be mutated, so copying an OwnerID is safe across
// request, result, cache, and asynchronous boundaries.
type OwnerID struct {
	exact string
}

// Protocol identifiers have distinct Go types so two UUID-shaped values from
// different identity domains cannot be exchanged accidentally. Their textual
// UUID representation is a transport concern; the metastore compares bytes.
type ProducerID [16]byte
type ProducerIncarnationID [16]byte
type KafkaBindingID [16]byte
type KafkaTopicID [16]byte

const (
	// This domain is already used by UJPK routing. Its historical package name
	// is retained deliberately: renaming a Go package must not change durable
	// timeline hashes.
	timelineHashDomain   = "unijord/timeline-index/key/v2\x00"
	maxTimelineHashInput = len(timelineHashDomain) + 4 + MaxNamespaceBytes + 4 + MaxTimelineBytes
)

func CopyNamespace(exact []byte) Namespace {
	return TakeNamespace(bytes.Clone(exact))
}

// CopyOwnerID copies arbitrary owner bytes into an immutable value.
func CopyOwnerID(exact []byte) OwnerID {
	return OwnerID{exact: string(exact)}
}

// OwnerIDFromString constructs an immutable owner from an already immutable
// Go string. The string is treated as opaque bytes, not UTF-8 text.
func OwnerIDFromString(exact string) OwnerID {
	return OwnerID{exact: exact}
}

// Bytes returns a caller-owned copy.
func (o OwnerID) Bytes() []byte { return []byte(o.exact) }

func (o OwnerID) Len() int { return len(o.exact) }

func (o OwnerID) Equal(other OwnerID) bool { return o.exact == other.exact }

func ValidateOwnerID(owner OwnerID) error {
	if owner.Len() == 0 || owner.Len() > MaxOwnerBytes {
		return fmt.Errorf("%w: owner bytes=%d", ErrInvalidRequest, owner.Len())
	}
	return nil
}

// TakeNamespace consumes exact. The caller must not mutate exact afterward.
func TakeNamespace(exact []byte) Namespace {
	return Namespace{identity: &namespaceIdentity{exact: exact, digest: namespaceid.Sum(exact)}}
}

func (n Namespace) Bytes() []byte {
	if n.identity == nil {
		return nil
	}
	return n.identity.exact
}

func (n Namespace) Len() int { return len(n.Bytes()) }

func (n Namespace) Hash() [32]byte {
	if n.identity == nil {
		return [32]byte{}
	}
	return n.identity.digest
}

func (n Namespace) Equal(other Namespace) bool {
	if n.identity == other.identity {
		return true
	}
	return n.Hash() == other.Hash() && bytes.Equal(n.Bytes(), other.Bytes())
}

func CopyTimelineKey(namespace Namespace, exact []byte) TimelineKey {
	return TakeTimelineKey(namespace, bytes.Clone(exact))
}

// TakeTimelineKey consumes exact. The caller must not mutate exact afterward.
func TakeTimelineKey(namespace Namespace, exact []byte) TimelineKey {
	return TimelineKey{identity: &timelineIdentity{
		namespace: namespace,
		exact:     exact,
		digest:    hashTimeline(namespace.Bytes(), exact),
	}}
}

func (k TimelineKey) Namespace() Namespace {
	if k.identity == nil {
		return Namespace{}
	}
	return k.identity.namespace
}

func (k TimelineKey) Bytes() []byte {
	if k.identity == nil {
		return nil
	}
	return k.identity.exact
}

func (k TimelineKey) Hash() [32]byte {
	if k.identity == nil {
		return [32]byte{}
	}
	return k.identity.digest
}

func (k TimelineKey) Equal(other TimelineKey) bool {
	if k.identity == other.identity {
		return true
	}
	return k.Hash() == other.Hash() && k.Namespace().Equal(other.Namespace()) &&
		bytes.Equal(k.Bytes(), other.Bytes())
}

func ValidateNamespace(namespace Namespace) error {
	if namespace.Len() == 0 || namespace.Len() > MaxNamespaceBytes {
		return fmt.Errorf("%w: namespace bytes=%d", ErrInvalidRequest, namespace.Len())
	}
	if namespace.Hash() != namespaceid.Sum(namespace.Bytes()) {
		return fmt.Errorf("%w: namespace digest mismatch", ErrCorrupt)
	}
	return nil
}

func ValidateTimelineKey(key TimelineKey) error {
	if err := ValidateNamespace(key.Namespace()); err != nil {
		return err
	}
	if len(key.Bytes()) == 0 || len(key.Bytes()) > MaxTimelineBytes {
		return fmt.Errorf("%w: timeline bytes=%d", ErrInvalidRequest, len(key.Bytes()))
	}
	if key.Hash() != hashTimeline(key.Namespace().Bytes(), key.Bytes()) {
		return fmt.Errorf("%w: timeline digest mismatch", ErrCorrupt)
	}
	return nil
}

func ValidateShardKey(key ShardKey) error {
	return ValidateNamespace(key.Namespace)
}

func isZero128[T ~[16]byte](id T) bool {
	var zero T
	return id == zero
}

func hashTimeline(namespace, timeline []byte) [32]byte {
	if len(namespace) <= MaxNamespaceBytes && len(timeline) <= MaxTimelineBytes {
		var input [maxTimelineHashInput]byte
		offset := copy(input[:], timelineHashDomain)
		binary.BigEndian.PutUint32(input[offset:offset+4], uint32(len(namespace)))
		offset += 4
		offset += copy(input[offset:], namespace)
		binary.BigEndian.PutUint32(input[offset:offset+4], uint32(len(timeline)))
		offset += 4
		offset += copy(input[offset:], timeline)
		return sha256.Sum256(input[:offset])
	}

	h := sha256.New()
	_, _ = h.Write([]byte(timelineHashDomain))
	writeLengthBytes(h, namespace)
	writeLengthBytes(h, timeline)
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeLengthBytes(w byteWriter, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = w.Write(size[:])
	_, _ = w.Write(value)
}
