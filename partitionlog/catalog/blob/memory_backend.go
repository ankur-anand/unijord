package blob

import blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"

// MemoryBackend is an in-memory Backend for tests and local development.
type MemoryBackend = blobmemory.Store

func NewMemoryBackend() *MemoryBackend {
	return blobmemory.New()
}
