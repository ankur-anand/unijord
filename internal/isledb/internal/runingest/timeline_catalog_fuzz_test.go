package runingest

import "testing"

func FuzzTimelineCatalog(f *testing.F) {
	f.Add([]byte{1, 2, 3, 0, 255, 1, 32}, []byte{0, 0xff, 1, 0, 2}, false)
	f.Add([]byte{32, 32, 32, 255, 32}, []byte{0xff, 0xff, 0, 0}, true)
	f.Add([]byte{0, 1}, []byte{}, true)
	f.Fuzz(func(t *testing.T, operations, source []byte, collision bool) {
		// Bound parse work, model work, owned bytes, and collision probes before
		// construction. No input-directed unbounded allocation or iteration.
		if len(operations) > 128 || len(source) > 512 {
			t.Skip()
		}
		runCatalogModel(t, operations, source, collision)
	})
}
