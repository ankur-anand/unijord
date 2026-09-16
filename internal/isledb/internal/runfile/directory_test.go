package runfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestDirectoryCompatibilityVector(t *testing.T) {
	vector := loadOuterFramingVector(t)
	expected := decodeHex(t, vector.Directory.ExpectedHex)
	directory, err := UnmarshalDirectory(expected, vector.Directory.Offset)
	if err != nil {
		t.Fatal(err)
	}
	if directory.DirectoryOffset != 4408 || len(directory.Regions) != 3 {
		t.Fatalf("decoded directory offset=%d regions=%d", directory.DirectoryOffset, len(directory.Regions))
	}
	if !bytes.Equal(directory.MinTimeline, []byte{0x00}) || !bytes.Equal(directory.MaxTimeline, []byte{0xff, 0x10}) {
		t.Fatalf("timeline bounds=%x..%x", directory.MinTimeline, directory.MaxTimeline)
	}
	if got := directory.Regions[0]; got.Kind != RegionKindEventsSST || !got.Required || got.Encoding != RegionEncodingV1 || got.Offset != 128 || got.Length != 17 || got.EntryCount != 1 || got.SeqLo != 10 || got.SeqHi != 12 || !bytes.Equal(got.MinKey, []byte{0x10, 0x00}) || !bytes.Equal(got.MaxKey, []byte{0x10, 0xff}) {
		t.Fatalf("decoded Events descriptor=%+v", got)
	}
	if got := directory.Regions[1]; got.Kind != RegionKindHeadsSST || !got.Required || got.Encoding != RegionEncodingV1 || got.Offset != 152 || got.Length != 19 || got.EntryCount != 1 || got.SeqLo != 12 || got.SeqHi != 12 || !bytes.Equal(got.MinKey, []byte{0x20, 0x00}) || !bytes.Equal(got.MaxKey, []byte{0x20, 0xff}) {
		t.Fatalf("decoded Heads descriptor=%+v", got)
	}
	if got := directory.Regions[2]; got.Kind != RegionKindTimelineFilter || got.Required || got.Encoding != RegionEncodingV1 || got.Offset != 176 || got.Length != 4232 || got.EntryCount != 3277 || got.SeqLo != 0 || got.SeqHi != 0 || len(got.MinKey) != 0 || len(got.MaxKey) != 0 {
		t.Fatalf("decoded filter descriptor=%+v", got)
	}
	encoded, err := MarshalDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, expected) {
		t.Fatal("directory does not reproduce frozen compatibility bytes")
	}
	if uint64(len(encoded)) != vector.Directory.Length {
		t.Fatalf("directory length=%d, want %d", len(encoded), vector.Directory.Length)
	}
	if got := encodeHex(encoded[len(encoded)-len(decodeHex(t, vector.Directory.KeyBlobHex)):]); got != vector.Directory.KeyBlobHex {
		t.Fatalf("key blob=%s, want %s", got, vector.Directory.KeyBlobHex)
	}
	if got := sha256Hex(encoded); got != vector.Directory.ExpectedSHA256 {
		t.Fatalf("directory SHA-256=%s, want %s", got, vector.Directory.ExpectedSHA256)
	}
}

func TestDirectoryBoundaryRoundTrip(t *testing.T) {
	minimum := minimumDirectory()
	encoded, err := MarshalDirectory(minimum)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 326 {
		t.Fatalf("minimum directory length=%d, want 326", len(encoded))
	}
	decoded, err := UnmarshalDirectory(encoded, minimum.DirectoryOffset)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, minimum) {
		t.Fatal("minimum directory did not round trip")
	}

	maximum := maximumDirectory()
	encoded, err = MarshalDirectory(maximum)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 1_050_544 {
		t.Fatalf("maximum canonical directory length=%d, want 1050544", len(encoded))
	}
	decoded, err = UnmarshalDirectory(encoded, maximum.DirectoryOffset)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, maximum) {
		t.Fatal("maximum directory did not round trip")
	}
}

func TestDirectoryRejectsHeaderMutations(t *testing.T) {
	valid, offset := compatibilityDirectoryBytes(t)
	tests := []struct {
		name   string
		want   error
		mutate func([]byte)
	}{
		{name: "magic", want: ErrCorruptRun, mutate: func(data []byte) { data[0] ^= 1 }},
		{name: "version", want: ErrUnsupportedRunVersion, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[4:6], 2) }},
		{name: "header bytes", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[6:8], 63) }},
		{name: "few regions", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[8:10], MinRegionCount-1) }},
		{name: "many regions", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[8:10], MaxRegionCount+1) }},
		{name: "entry bytes", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[10:12], RegionDescriptorBytes-1) }},
		{name: "flags", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[12:16], 1) }},
		{name: "entries offset", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[16:24], DirectoryHeaderBytes+1) }},
		{name: "entries length", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[24:32], 0) }},
		{name: "key blob offset", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[32:40], 0) }},
		{name: "key blob length", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[40:48], 0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			if _, err := UnmarshalDirectory(data, offset); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}

	for _, length := range []int{0, DirectoryHeaderBytes - 1} {
		if _, err := UnmarshalDirectory(valid[:length], offset); !errors.Is(err, ErrCorruptRun) {
			t.Fatalf("length %d error=%v, want corruption", length, err)
		}
	}
	if _, err := UnmarshalDirectory(make([]byte, MaxDirectoryBytes+1), offset); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("oversized error=%v, want too large", err)
	}
	if _, err := UnmarshalDirectory(valid, PreambleBytes-1); !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("early offset error=%v, want corruption", err)
	}
	if _, err := UnmarshalDirectory(valid, offset+1); !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("unaligned offset error=%v, want corruption", err)
	}
	if _, err := UnmarshalDirectory(valid, math.MaxUint64-7); !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("overflowing offset error=%v, want corruption", err)
	}
	if _, err := UnmarshalDirectory(valid, MaxRunObjectBytes); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("oversized placement error=%v, want too large", err)
	}
}

func TestDirectoryRejectsDescriptorMutations(t *testing.T) {
	valid, offset := compatibilityDirectoryBytes(t)
	const first = DirectoryHeaderBytes
	const second = DirectoryHeaderBytes + RegionDescriptorBytes
	const third = DirectoryHeaderBytes + 2*RegionDescriptorBytes
	tests := []struct {
		name   string
		want   error
		mutate func([]byte)
	}{
		{name: "unknown flags", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[first+2:first+4], 3) }},
		{name: "reserved0", want: ErrCorruptRun, mutate: func(data []byte) { data[first+6] = 1 }},
		{name: "reserved1", want: ErrCorruptRun, mutate: func(data []byte) { data[first+96] = 1 }},
		{name: "duplicate kind", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[second:second+2], uint16(RegionKindEventsSST)) }},
		{name: "kind order", want: ErrCorruptRun, mutate: func(data []byte) {
			binary.BigEndian.PutUint16(data[first:first+2], uint16(RegionKindHeadsSST))
			binary.BigEndian.PutUint16(data[second:second+2], uint16(RegionKindEventsSST))
		}},
		{name: "kind zero", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[first:first+2], 0) }},
		{name: "unknown required kind", want: ErrUnsupportedRunVersion, mutate: func(data []byte) {
			binary.BigEndian.PutUint16(data[third:third+2], 4)
			binary.BigEndian.PutUint16(data[third+2:third+4], regionFlagRequired)
		}},
		{name: "required encoding", want: ErrUnsupportedRunVersion, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[first+4:first+6], 2) }},
		{name: "required bit missing", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[first+2:first+4], 0) }},
		{name: "filter required", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[third+2:third+4], regionFlagRequired) }},
		{name: "before preamble", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[first+8:first+16], PreambleBytes-8) }},
		{name: "unaligned", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[first+8:first+16], PreambleBytes+1) }},
		{name: "zero length", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[first+16:first+24], 0) }},
		{name: "range overflow", want: ErrCorruptRun, mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[first+8:first+16], math.MaxUint64-7)
			binary.BigEndian.PutUint64(data[first+16:first+24], 16)
		}},
		{name: "crosses directory", want: ErrCorruptRun, mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[third+8:third+16], offset-8)
			binary.BigEndian.PutUint64(data[third+16:third+24], 16)
		}},
		{name: "overlap", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[second+8:second+16], 144) }},
		{name: "physical order", want: ErrCorruptRun, mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[first+8:first+16], 160)
			binary.BigEndian.PutUint64(data[first+16:first+24], 8)
			binary.BigEndian.PutUint64(data[second+8:second+16], 128)
			binary.BigEndian.PutUint64(data[second+16:second+24], 8)
		}},
		{name: "zero hash", want: ErrCorruptRun, mutate: func(data []byte) { clear(data[first+64 : first+96]) }},
		{name: "empty required region", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[first+24:first+32], 0) }},
		{name: "inverted sequence", want: ErrCorruptRun, mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[first+32:first+40], 13)
			binary.BigEndian.PutUint64(data[first+40:first+48], 12)
		}},
		{name: "zero filter entries", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[third+24:third+32], 0) }},
		{name: "filter sequence", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[third+32:third+40], 1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			if _, err := UnmarshalDirectory(data, offset); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestDirectoryUnknownOptionalRegionAccepted(t *testing.T) {
	const third = DirectoryHeaderBytes + 2*RegionDescriptorBytes
	tests := []struct {
		name   string
		mutate func([]byte)
		check  func(RegionDescriptor) bool
	}{
		{
			name: "unknown kind and encoding",
			mutate: func(data []byte) {
				binary.BigEndian.PutUint16(data[third:third+2], 4)
				binary.BigEndian.PutUint16(data[third+4:third+6], math.MaxUint16)
			},
			check: func(region RegionDescriptor) bool {
				return region.Kind == 4 && !region.Required && region.Encoding == RegionEncoding(math.MaxUint16)
			},
		},
		{
			name: "known optional kind with unknown encoding",
			mutate: func(data []byte) {
				binary.BigEndian.PutUint16(data[third+4:third+6], 2)
			},
			check: func(region RegionDescriptor) bool {
				return region.Kind == RegionKindTimelineFilter && !region.Required && region.Encoding == 2
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			valid, offset := compatibilityDirectoryBytes(t)
			test.mutate(valid)
			directory, err := UnmarshalDirectory(valid, offset)
			if err != nil {
				t.Fatal(err)
			}
			if got := directory.Regions[2]; !test.check(got) {
				t.Fatalf("unknown optional descriptor=%+v", got)
			}
			encoded, err := MarshalDirectory(directory)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, valid) {
				t.Fatal("unknown optional region did not round trip")
			}
		})
	}
}

func TestDirectoryRejectsMalformedKeyBlob(t *testing.T) {
	valid, offset := compatibilityDirectoryBytes(t)
	const first = DirectoryHeaderBytes
	tests := []struct {
		name   string
		want   error
		mutate func([]byte) []byte
	}{
		{name: "empty timeline", want: ErrCorruptRun, mutate: func(data []byte) []byte { binary.BigEndian.PutUint32(data[52:56], 0); return data }},
		{name: "long timeline", mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[52:56], uint32(MaxTimelineBytes+1))
			return data
		}, want: ErrRunTooLarge},
		{name: "timeline out of bounds", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[48:52], uint32(len(data)))
			return data
		}},
		{name: "noncanonical timeline", want: ErrCorruptRun, mutate: func(data []byte) []byte { binary.BigEndian.PutUint32(data[48:52], 1); return data }},
		{name: "inverted timelines", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			keyBlob := int(binary.BigEndian.Uint64(data[32:40]))
			data[keyBlob] = 0xff
			data[keyBlob+1] = 0x00
			return data
		}},
		{name: "empty key with offset", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[first+48:first+52], 1)
			binary.BigEndian.PutUint32(data[first+52:first+56], 0)
			return data
		}},
		{name: "long key", mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[first+52:first+56], uint32(MaxTableKeyBytes+1))
			return data
		}, want: ErrRunTooLarge},
		{name: "key out of bounds", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[first+48:first+52], 10)
			binary.BigEndian.PutUint32(data[first+52:first+56], 2)
			return data
		}},
		{name: "noncanonical key", want: ErrCorruptRun, mutate: func(data []byte) []byte { binary.BigEndian.PutUint32(data[first+48:first+52], 4); return data }},
		{name: "missing required key", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[first+48:first+52], 0)
			binary.BigEndian.PutUint32(data[first+52:first+56], 0)
			return data
		}},
		{name: "inverted required keys", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			keyBlob := int(binary.BigEndian.Uint64(data[32:40]))
			data[keyBlob+3], data[keyBlob+4] = 0xff, 0xff
			data[keyBlob+5], data[keyBlob+6] = 0x00, 0x00
			return data
		}},
		{name: "trailing byte", want: ErrCorruptRun, mutate: func(data []byte) []byte {
			data = append(data, 0)
			binary.BigEndian.PutUint64(data[40:48], binary.BigEndian.Uint64(data[40:48])+1)
			return data
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := test.mutate(bytes.Clone(valid))
			if _, err := UnmarshalDirectory(data, offset); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestDirectoryMarshalRejectsInvalidInput(t *testing.T) {
	validBytes, offset := compatibilityDirectoryBytes(t)
	valid, err := UnmarshalDirectory(validBytes, offset)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		want   error
		mutate func(*Directory)
	}{
		{name: "directory before preamble", want: ErrInvalidRun, mutate: func(d *Directory) { d.DirectoryOffset = PreambleBytes - 1 }},
		{name: "unaligned directory", want: ErrInvalidRun, mutate: func(d *Directory) { d.DirectoryOffset++ }},
		{name: "few regions", want: ErrInvalidRun, mutate: func(d *Directory) { d.Regions = d.Regions[:1] }},
		{name: "many regions", want: ErrInvalidRun, mutate: func(d *Directory) {
			for len(d.Regions) <= int(MaxRegionCount) {
				region := d.Regions[len(d.Regions)-1]
				region.Kind++
				region.Offset += RegionAlignment
				d.Regions = append(d.Regions, region)
			}
		}},
		{name: "missing Events", want: ErrInvalidRun, mutate: func(d *Directory) {
			d.Regions = append([]RegionDescriptor(nil), d.Regions[1:]...)
			d.Regions[1].Kind = 4
		}},
		{name: "filter key bounds", want: ErrInvalidRun, mutate: func(d *Directory) { d.Regions[2].MinKey = []byte{1} }},
		{name: "long timeline", want: ErrRunTooLarge, mutate: func(d *Directory) { d.MinTimeline = make([]byte, MaxTimelineBytes+1) }},
		{name: "long key", want: ErrRunTooLarge, mutate: func(d *Directory) { d.Regions[0].MinKey = make([]byte, MaxTableKeyBytes+1) }},
		{name: "directory range overflow", want: ErrInvalidRun, mutate: func(d *Directory) { d.DirectoryOffset = math.MaxUint64 - 7 }},
		{name: "object too large", want: ErrRunTooLarge, mutate: func(d *Directory) { d.DirectoryOffset = MaxRunObjectBytes }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneDirectory(valid)
			test.mutate(&candidate)
			if _, err := MarshalDirectory(candidate); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestDirectoryDecodeOwnsCallerData(t *testing.T) {
	data, offset := compatibilityDirectoryBytes(t)
	directory, err := UnmarshalDirectory(data, offset)
	if err != nil {
		t.Fatal(err)
	}
	want := cloneDirectory(directory)
	clear(data)
	if !reflect.DeepEqual(directory, want) {
		t.Fatal("decoded directory aliases caller input")
	}
	directory.MinTimeline[0] ^= 0xff
	if reflect.DeepEqual(directory.MinTimeline, want.MinTimeline) || !bytes.Equal(directory.MaxTimeline, want.MaxTimeline) || !bytes.Equal(directory.Regions[0].MinKey, want.Regions[0].MinKey) {
		t.Fatal("decoded directory byte fields do not have independent ownership")
	}
}

func compatibilityDirectoryBytes(t *testing.T) ([]byte, uint64) {
	t.Helper()
	vector := loadOuterFramingVector(t)
	return decodeHex(t, vector.Directory.ExpectedHex), vector.Directory.Offset
}

func minimumDirectory() Directory {
	regions := []RegionDescriptor{
		{
			Kind:       RegionKindEventsSST,
			Required:   true,
			Encoding:   RegionEncodingV1,
			Offset:     PreambleBytes,
			Length:     RegionAlignment,
			EntryCount: 1,
			MinKey:     []byte{0},
			MaxKey:     []byte{0},
		},
		{
			Kind:       RegionKindHeadsSST,
			Required:   true,
			Encoding:   RegionEncodingV1,
			Offset:     PreambleBytes + RegionAlignment,
			Length:     RegionAlignment,
			EntryCount: 1,
			MinKey:     []byte{0},
			MaxKey:     []byte{0},
		},
	}
	for i := range regions {
		regions[i].ContentHash[0] = byte(i + 1)
	}
	return Directory{
		DirectoryOffset: PreambleBytes + 2*RegionAlignment,
		MinTimeline:     []byte{0},
		MaxTimeline:     []byte{0},
		Regions:         regions,
	}
}

func maximumDirectory() Directory {
	kinds := []RegionKind{RegionKindEventsSST, RegionKindHeadsSST, 4, 5, 6, 7, 8, RegionKind(math.MaxUint16)}
	regions := make([]RegionDescriptor, len(kinds))
	for i, kind := range kinds {
		regions[i] = RegionDescriptor{
			Kind:       kind,
			Encoding:   RegionEncoding(math.MaxUint16),
			Offset:     PreambleBytes + uint64(i)*RegionAlignment,
			Length:     RegionAlignment,
			EntryCount: math.MaxUint64,
			SeqHi:      math.MaxUint64,
			MinKey:     bytes.Repeat([]byte{0x00}, int(MaxTableKeyBytes)),
			MaxKey:     bytes.Repeat([]byte{0xff}, int(MaxTableKeyBytes)),
		}
		regions[i].ContentHash[0] = byte(i + 1)
	}
	for i := 0; i < 2; i++ {
		regions[i].Required = true
		regions[i].Encoding = RegionEncodingV1
	}
	return Directory{
		DirectoryOffset: PreambleBytes + uint64(len(regions))*RegionAlignment,
		MinTimeline:     bytes.Repeat([]byte{0x00}, int(MaxTimelineBytes)),
		MaxTimeline:     bytes.Repeat([]byte{0xff}, int(MaxTimelineBytes)),
		Regions:         regions,
	}
}

func cloneDirectory(source Directory) Directory {
	clone := source
	clone.MinTimeline = bytes.Clone(source.MinTimeline)
	clone.MaxTimeline = bytes.Clone(source.MaxTimeline)
	clone.Regions = make([]RegionDescriptor, len(source.Regions))
	for i := range source.Regions {
		clone.Regions[i] = source.Regions[i]
		clone.Regions[i].MinKey = bytes.Clone(source.Regions[i].MinKey)
		clone.Regions[i].MaxKey = bytes.Clone(source.Regions[i].MaxKey)
	}
	return clone
}
