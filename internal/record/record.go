package record

const (
	MaxTimelineKeyBytes = 512
	MaxRecordValueBytes = 4 << 20
	MaxHeaderBytes      = 256 << 10
	MaxHeaders          = 64
	MaxHeaderKeyBytes   = 1024
	MaxHeaderValueBytes = 65535
)

type Header struct {
	Key   []byte
	Value []byte
}

// Record has already been assigned its public timeline-local position.
// Physical codecs validate this position; they never allocate it.
type Record struct {
	TimelineKey []byte
	TimelineLSN uint64
	TimestampMS int64
	Headers     []Header
	Value       []byte
}
