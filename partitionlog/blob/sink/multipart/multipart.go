package multipart

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	defaultContentType = "application/octet-stream"

	// Azure metadata names must be identifiers, so the shared names use only
	// ASCII letters and underscores accepted by every provider.
	MetadataSessionID = "unijord_upload_session"
	MetadataSize      = "unijord_object_size"
	MetadataSHA256    = "unijord_object_sha256"
)

var (
	ErrInvalidStore        = errors.New("blob/sink/multipart: invalid store request")
	ErrCleaned             = errors.New("blob/sink/multipart: session cleaned")
	ErrCommitted           = errors.New("blob/sink/multipart: session committed")
	ErrPartConflict        = errors.New("blob/sink/multipart: part identity conflict")
	ErrCommitInProgress    = errors.New("blob/sink/multipart: commit in progress")
	ErrCommitIndeterminate = errors.New("blob/sink/multipart: commit outcome indeterminate")
	ErrPreconditionFailed  = errors.New("blob/sink/multipart: final object precondition failed")
)

// Store begins provider sessions and describes their hard multipart limits.
type Store interface {
	Limits() Limits
	// Begin must return promptly when ctx is canceled. The returned session owns
	// its lifetime; cancellation of ctx after Begin returns does not clean it.
	Begin(ctx context.Context, key string, opts Options) (Session, error)
}

// Session is the provider primitive used by the common streaming layer.
//
// PutPart must be safe for concurrent calls with different part numbers. A
// retry of the same number and checksum must be safe; different content for an
// existing number returns ErrPartConflict. Commit is retryable and reconciles
// an already-created final object using the session identity. Cleanup only
// discards staging work: it never promises that the final object is absent.
type Session interface {
	Limits() Limits
	PutPart(context.Context, Part) (Receipt, error)
	Commit(context.Context, CommitRequest) (ObjectAttrs, error)
	Cleanup(context.Context) error
}

// Limits contains object assembly limits enforced by the adapter. They may be
// stricter than the provider's hard limits. MinPartSize does not apply to the
// final part.
type Limits struct {
	MinPartSize   uint64
	MaxPartSize   uint64
	MaxPartCount  int
	MaxObjectSize uint64
}

func (l Limits) Validate() error {
	if l.MaxPartSize == 0 {
		return fmt.Errorf("%w: max_part_size must be positive", ErrInvalidStore)
	}
	if l.MinPartSize > l.MaxPartSize {
		return fmt.Errorf("%w: min_part_size=%d exceeds max_part_size=%d", ErrInvalidStore, l.MinPartSize, l.MaxPartSize)
	}
	if l.MaxPartCount <= 0 {
		return fmt.Errorf("%w: max_part_count must be positive", ErrInvalidStore)
	}
	if l.MaxObjectSize == 0 {
		return fmt.Errorf("%w: max_object_size must be positive", ErrInvalidStore)
	}
	return nil
}

type Options struct {
	ContentType string

	// StagingPrefix is a root supplied by the caller. NormalizeOptions appends
	// the unique SessionID so different attempts never share staging keys.
	StagingPrefix string
	// SessionID identifies one logical final-object creation attempt. When
	// empty, NormalizeOptions generates a Google UUID v4.
	SessionID string
	// Metadata is copied onto the final object. Keys used by this package are
	// reserved and cannot be supplied by callers.
	Metadata map[string]string
}

type Part struct {
	Number         int
	Bytes          []byte
	ChecksumSHA256 [sha256.Size]byte
}

type Receipt struct {
	Number         int
	Token          string
	SizeBytes      uint64
	ChecksumSHA256 [sha256.Size]byte
}

type CommitRequest struct {
	Receipts     []Receipt
	SizeBytes    uint64
	ObjectSHA256 [sha256.Size]byte
}

type ObjectAttrs struct {
	Key          string
	SizeBytes    uint64
	Token        string
	SessionID    string
	ObjectSHA256 [sha256.Size]byte
}

func NormalizeOptions(key string, opts Options) (Options, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Options{}, fmt.Errorf("%w: empty key", ErrInvalidStore)
	}
	if opts.ContentType == "" {
		opts.ContentType = defaultContentType
	}
	if opts.SessionID == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return Options{}, fmt.Errorf("%w: generate session id: %w", ErrInvalidStore, err)
		}
		opts.SessionID = id.String()
	} else {
		id, err := uuid.Parse(opts.SessionID)
		if err != nil || id == uuid.Nil || id.Version() != 4 || id.String() != strings.ToLower(opts.SessionID) {
			return Options{}, fmt.Errorf("%w: invalid canonical UUID v4 session id %q", ErrInvalidStore, opts.SessionID)
		}
	}
	for _, reserved := range []string{MetadataSessionID, MetadataSize, MetadataSHA256} {
		for key := range opts.Metadata {
			if strings.EqualFold(key, reserved) {
				return Options{}, fmt.Errorf("%w: metadata key %q is reserved", ErrInvalidStore, key)
			}
		}
	}
	opts.Metadata = cloneMetadata(opts.Metadata)
	root := strings.TrimSuffix(opts.StagingPrefix, "/")
	if root == "" {
		root = key + ".staging"
	}
	opts.StagingPrefix = path.Join(root, opts.SessionID)
	return opts, nil
}

func NewPart(number int, bytes []byte) Part {
	return Part{Number: number, Bytes: bytes, ChecksumSHA256: sha256.Sum256(bytes)}
}

func ValidatePart(part Part) error {
	if part.Number <= 0 {
		return fmt.Errorf("%w: invalid part number %d", ErrInvalidStore, part.Number)
	}
	if len(part.Bytes) == 0 {
		return fmt.Errorf("%w: empty part %d", ErrInvalidStore, part.Number)
	}
	actual := sha256.Sum256(part.Bytes)
	if actual != part.ChecksumSHA256 {
		return fmt.Errorf("%w: part %d checksum mismatch", ErrPartConflict, part.Number)
	}
	return nil
}

func ValidatePartLimits(part Part, limits Limits) error {
	if err := ValidatePart(part); err != nil {
		return err
	}
	if part.Number > limits.MaxPartCount {
		return fmt.Errorf("%w: part number=%d max=%d", ErrInvalidStore, part.Number, limits.MaxPartCount)
	}
	if uint64(len(part.Bytes)) > limits.MaxPartSize {
		return fmt.Errorf("%w: part %d size=%d max=%d", ErrInvalidStore, part.Number, len(part.Bytes), limits.MaxPartSize)
	}
	return nil
}

func NewCommitRequest(receipts []Receipt) CommitRequest {
	request := CommitRequest{Receipts: append([]Receipt(nil), receipts...)}
	for _, receipt := range receipts {
		request.SizeBytes += receipt.SizeBytes
	}
	return request
}

func ValidateCommitRequest(request CommitRequest, limits Limits) error {
	if err := ValidateReceipts(request.Receipts); err != nil {
		return err
	}
	if len(request.Receipts) > limits.MaxPartCount {
		return fmt.Errorf("%w: receipt count=%d max=%d", ErrInvalidStore, len(request.Receipts), limits.MaxPartCount)
	}
	var size uint64
	for i, receipt := range request.Receipts {
		if receipt.SizeBytes > limits.MaxPartSize {
			return fmt.Errorf("%w: part %d receipt size=%d max=%d", ErrInvalidStore, receipt.Number, receipt.SizeBytes, limits.MaxPartSize)
		}
		if i < len(request.Receipts)-1 && receipt.SizeBytes < limits.MinPartSize {
			return fmt.Errorf("%w: non-final part %d size=%d min=%d", ErrInvalidStore, receipt.Number, receipt.SizeBytes, limits.MinPartSize)
		}
		if math.MaxUint64-size < receipt.SizeBytes {
			return fmt.Errorf("%w: object size overflow", ErrInvalidStore)
		}
		size += receipt.SizeBytes
	}
	if size != request.SizeBytes {
		return fmt.Errorf("%w: request size=%d receipt size=%d", ErrInvalidStore, request.SizeBytes, size)
	}
	if size > limits.MaxObjectSize {
		return fmt.Errorf("%w: object size=%d max=%d", ErrInvalidStore, size, limits.MaxObjectSize)
	}
	return nil
}

func ValidateReceipts(receipts []Receipt) error {
	if len(receipts) == 0 {
		return fmt.Errorf("%w: no receipts", ErrInvalidStore)
	}
	for i, receipt := range receipts {
		want := i + 1
		if receipt.Number != want {
			return fmt.Errorf("%w: receipt number=%d want=%d", ErrInvalidStore, receipt.Number, want)
		}
		if receipt.SizeBytes == 0 {
			return fmt.Errorf("%w: empty receipt part %d", ErrInvalidStore, receipt.Number)
		}
	}
	return nil
}

func CommitMetadata(opts Options, request CommitRequest) map[string]string {
	metadata := cloneMetadata(opts.Metadata)
	metadata[MetadataSessionID] = opts.SessionID
	metadata[MetadataSize] = strconv.FormatUint(request.SizeBytes, 10)
	if request.ObjectSHA256 != ([sha256.Size]byte{}) {
		metadata[MetadataSHA256] = hex.EncodeToString(request.ObjectSHA256[:])
	}
	return metadata
}

func SessionMetadata(opts Options) map[string]string {
	metadata := cloneMetadata(opts.Metadata)
	metadata[MetadataSessionID] = opts.SessionID
	return metadata
}

func MatchesCommittedObject(size uint64, metadata map[string]string, opts Options, request CommitRequest, requireChecksum bool) bool {
	if size != request.SizeBytes || metadataValue(metadata, MetadataSessionID) != opts.SessionID {
		return false
	}
	if encodedSize := metadataValue(metadata, MetadataSize); encodedSize != "" && encodedSize != strconv.FormatUint(request.SizeBytes, 10) {
		return false
	}
	if request.ObjectSHA256 != ([sha256.Size]byte{}) {
		encodedHash := metadataValue(metadata, MetadataSHA256)
		if requireChecksum && encodedHash == "" {
			return false
		}
		if encodedHash != "" && encodedHash != hex.EncodeToString(request.ObjectSHA256[:]) {
			return false
		}
	}
	return true
}

func StagingPartKey(stagingPrefix string, number int, attemptID string) string {
	return path.Join(stagingPrefix, "parts", fmt.Sprintf("%06d", number), attemptID)
}

func StagingComposePrefix(stagingPrefix, attemptID string) string {
	return path.Join(stagingPrefix, "compose", attemptID)
}

func cloneMetadata(metadata map[string]string) map[string]string {
	clone := make(map[string]string, len(metadata)+3)
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func metadataValue(metadata map[string]string, name string) string {
	for key, value := range metadata {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}
