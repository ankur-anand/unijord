package segreader

import (
	"bytes"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestRecordCloneDeepCopiesHeadersAndValue(t *testing.T) {
	t.Parallel()

	record := Record{
		Partition:   7,
		LSN:         11,
		TimestampMS: 123,
		Headers: []segformat.Header{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
		Value: []byte("payload"),
	}

	clone := record.Clone()
	record.Headers[0].Key[0] = 'x'
	record.Headers[0].Value[0] = 'x'
	record.Value[0] = 'x'

	if clone.Partition != 7 || clone.LSN != 11 || clone.TimestampMS != 123 {
		t.Fatalf("clone metadata = partition:%d lsn:%d ts:%d, want partition:7 lsn:11 ts:123",
			clone.Partition, clone.LSN, clone.TimestampMS)
	}
	if !bytes.Equal(clone.Headers[0].Key, []byte("k1")) {
		t.Fatalf("clone header key = %q, want k1", clone.Headers[0].Key)
	}
	if !bytes.Equal(clone.Headers[0].Value, []byte("v1")) {
		t.Fatalf("clone header value = %q, want v1", clone.Headers[0].Value)
	}
	if !bytes.Equal(clone.Value, []byte("payload")) {
		t.Fatalf("clone value = %q, want payload", clone.Value)
	}
}
