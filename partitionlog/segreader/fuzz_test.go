package segreader

import (
	"context"
	"os"
	"testing"
)

func FuzzOpenAndScanSegment(f *testing.F) {
	seed, err := os.ReadFile(compatibilityCorpusPath("v2-none-crc32c.plseg"))
	if err != nil {
		f.Fatalf("read compatibility seed: %v", err)
	}
	f.Add(seed)

	f.Fuzz(func(t *testing.T, object []byte) {
		if len(object) > 32<<20 {
			t.Skip()
		}
		ref, ok := compatibilityRefFromObject(object)
		if !ok {
			return
		}
		store := newMemoryStore(map[string][]byte{ref.URI: object})
		reader, err := Open(context.Background(), store, ref, DefaultOptions())
		if err != nil {
			return
		}
		_, _ = reader.Read(context.Background(), ref.BaseLSN, 4096)
	})
}
