package keylayout

import "testing"

func TestSegmentObjectKey(t *testing.T) {
	t.Parallel()

	uuid := [16]byte{0x7c, 0x36, 0xf7, 0xc5, 0x97, 0x62, 0x4c, 0x59, 0xbd, 0x07, 0x60, 0x2d, 0x3b, 0xd8, 0x0a, 0xcc}
	got := SegmentObjectKey("plogload/history-100000", "plogload/history", 0, 300000, 1, uuid)
	want := "plogload/history-100000/segments/aac/streams/3db93f592226f81913bf9780557b06c91b2e0fde12f844c9b26fd6e68676e82f/p00000000/seg-00000000000000300000-e00000000000000000001-7c36f7c597624c59bd07602d3bd80acc.plseg"
	if got != want {
		t.Fatalf("SegmentObjectKey() = %q, want %q", got, want)
	}
}
