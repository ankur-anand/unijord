package blob

import (
	"errors"

	"github.com/ankur-anand/unijord/internal/blobstore"
)

var (
	ErrObjectNotFound    = blobstore.ErrObjectNotFound
	ErrImmutableConflict = blobstore.ErrImmutableConflict
	ErrCorruptCatalog    = blobstore.ErrInvalidRequest
	ErrIndexFull         = errors.New("catalog/blob: index full")
)

const (
	DefaultObjectListLimit = blobstore.DefaultListLimit
	MaxObjectListLimit     = blobstore.MaxListLimit
	ObjectContentType      = blobstore.BinaryContentType
)

// Backend is the minimal blob/object-store protocol required by the catalog.
//
// Put is for immutable page candidates. Implementations should make identical
// replays idempotent and reject the same key with different bytes.
//
// CompareAndSwap is for the mutable partition head. expectedToken == "" means
// create-if-absent. When the comparison fails and the object exists, it returns
// the current object with swapped=false so callers can reconcile without a
// second Get.
//
// Delete is used by bounded catalog-page GC. It should be idempotent for
// missing objects.
type Backend = blobstore.Store
type Object = blobstore.Object
type ObjectInfo = blobstore.ObjectInfo
type ListOptions = blobstore.ListOptions
type ObjectPage = blobstore.ObjectPage
