package catengine

import "github.com/ankur-anand/unijord/internal/blobstore"

// ErrCorruptCatalog is shared with the public blob catalog so durable format
// and reference failures retain their established errors.Is classification.
var ErrCorruptCatalog = blobstore.ErrInvalidRequest
