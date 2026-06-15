package keylayout

import "testing"

func TestBucketVectors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		streamID  string
		partition uint32
		checksum  uint32
		bucket    string
	}{
		{streamID: "hosts/host-a/events", partition: 0, checksum: 0x77483f93, bucket: "f93"},
		{streamID: "hosts/host-a/events", partition: 1, checksum: 0x8523bc90, bucket: "c90"},
		{streamID: "hosts/host-a/events", partition: 7, checksum: 0xa3825b78, bucket: "b78"},
		{streamID: "hosts/host-b/events", partition: 0, checksum: 0x24da43c3, bucket: "3c3"},
		{streamID: "agents/a7/sessions/42", partition: 0, checksum: 0x818e05ce, bucket: "5ce"},
		{streamID: "workflows/w19/events", partition: 0, checksum: 0x67e396aa, bucket: "6aa"},
		{streamID: "", partition: 1, checksum: 0x7e433189, bucket: "189"},
		{streamID: "", partition: 3, checksum: 0x9f78417e, bucket: "17e"},
		{streamID: "", partition: 7, checksum: 0x58e2d661, bucket: "661"},
	}
	for _, tc := range cases {
		if got := Checksum(tc.streamID, tc.partition); got != tc.checksum {
			t.Fatalf("Checksum(%q, %d) = %#08x, want %#08x", tc.streamID, tc.partition, got, tc.checksum)
		}
		if got := Bucket(tc.streamID, tc.partition); got != tc.bucket {
			t.Fatalf("Bucket(%q, %d) = %q, want %q", tc.streamID, tc.partition, got, tc.bucket)
		}
	}
}

func TestBucketConstants(t *testing.T) {
	t.Parallel()

	if BucketBits != 12 {
		t.Fatalf("BucketBits = %d, want 12", BucketBits)
	}
	if BucketHexLen != 3 {
		t.Fatalf("BucketHexLen = %d, want 3", BucketHexLen)
	}
	if BucketCount != 4096 {
		t.Fatalf("BucketCount = %d, want 4096", BucketCount)
	}
}
