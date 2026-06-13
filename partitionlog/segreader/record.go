package segreader

import "github.com/ankur-anand/unijord/partitionlog/segformat"

type Record struct {
	Partition   uint32
	LSN         uint64
	TimestampMS int64
	Headers     []segformat.Header
	Value       []byte
}

func (r Record) Clone() Record {
	out := r
	out.Headers = segformat.CloneHeaders(r.Headers)
	if len(r.Value) > 0 {
		out.Value = append([]byte(nil), r.Value...)
	}
	return out
}
