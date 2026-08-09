package keylayout

import (
	"errors"
	"strings"
	"testing"
)

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
	if StreamKeyHexLen != 64 {
		t.Fatalf("StreamKeyHexLen = %d, want 64", StreamKeyHexLen)
	}
	if MaxStreamIDBytes != 512 {
		t.Fatalf("MaxStreamIDBytes = %d, want 512", MaxStreamIDBytes)
	}
}

func TestCanonicalStreamID(t *testing.T) {
	t.Parallel()

	got, err := CanonicalStreamID("/hosts/host-a/events/")
	if err != nil {
		t.Fatalf("CanonicalStreamID() error = %v", err)
	}
	if got != "hosts/host-a/events" {
		t.Fatalf("CanonicalStreamID() = %q", got)
	}

	for _, streamID := range []string{"", "///", string([]byte{0xff}), strings.Repeat("x", MaxStreamIDBytes+1)} {
		if _, err := CanonicalStreamID(streamID); !errors.Is(err, ErrInvalidStreamID) {
			t.Fatalf("CanonicalStreamID(%q) error = %v, want %v", streamID, err, ErrInvalidStreamID)
		}
	}
}

func TestBucketNormalizesStreamID(t *testing.T) {
	t.Parallel()

	const streamID = "hosts/host-a/events"
	if got, want := Bucket("/"+streamID+"/", 7), Bucket(streamID, 7); got != want {
		t.Fatalf("Bucket() with slashes = %q, want %q", got, want)
	}
	if got, want := Checksum("/"+streamID+"/", 7), Checksum(streamID, 7); got != want {
		t.Fatalf("Checksum() with slashes = %#08x, want %#08x", got, want)
	}
	if got := NormalizeStreamID("/" + streamID + "/"); got != streamID {
		t.Fatalf("NormalizeStreamID() = %q, want %q", got, streamID)
	}
}

func TestStreamKey(t *testing.T) {
	t.Parallel()

	const want = "645c418edae21662304240f5181b1b63c713bfc0b062a2c3b1b84387aa786c91"
	if got := StreamKey("hosts/host-a/events"); got != want {
		t.Fatalf("StreamKey() = %q, want %q", got, want)
	}
	if got := StreamKey("/hosts/host-a/events/"); got != want {
		t.Fatalf("StreamKey() with slashes = %q, want %q", got, want)
	}
	if got := len(StreamKey("")); got != StreamKeyHexLen {
		t.Fatalf("len(StreamKey(empty)) = %d, want %d", got, StreamKeyHexLen)
	}
}
