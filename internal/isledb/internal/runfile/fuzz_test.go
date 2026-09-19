package runfile

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func fuzzCorpus(f *testing.F) [][]byte {
	f.Helper()
	var out [][]byte
	for _, c := range corpusCases {
		b, err := os.ReadFile(filepath.Join(compatDir, c.Name+".run"))
		if err != nil {
			f.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

func FuzzPreamble(f *testing.F) {
	for _, b := range fuzzCorpus(f) {
		f.Add(b[:PreambleBytes])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > PreambleBytes {
			return
		}
		v, err := UnmarshalPreamble(b)
		if err != nil {
			return
		}
		out, err := MarshalPreamble(v)
		if err != nil || !bytes.Equal(out, b) {
			t.Fatal("preamble canonical roundtrip", err)
		}
	})
}
func FuzzTrailer(f *testing.F) {
	for _, b := range fuzzCorpus(f) {
		f.Add(b[len(b)-TrailerBytes:])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > TrailerBytes {
			return
		}
		v, err := UnmarshalTrailer(b)
		if err != nil {
			return
		}
		out, err := MarshalTrailer(v)
		if err != nil || !bytes.Equal(out, b) {
			t.Fatal("trailer canonical roundtrip", err)
		}
	})
}
func FuzzDirectory(f *testing.F) {
	for _, b := range fuzzCorpus(f) {
		tr, err := UnmarshalTrailer(b[len(b)-TrailerBytes:])
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b[tr.DirectoryOffset:tr.DirectoryOffset+tr.DirectoryLength], tr.DirectoryOffset)
	}
	f.Fuzz(func(t *testing.T, b []byte, offset uint64) {
		// Decoder permits at most 8 descriptors and copies at most the capped
		// directory's key bytes. No count can allocate independently of this cap.
		if uint64(len(b)) > MaxDirectoryBytes || offset > MaxRunObjectBytes {
			return
		}
		v, err := UnmarshalDirectory(b, offset)
		if err != nil {
			return
		}
		out, err := MarshalDirectory(v)
		if err != nil || !bytes.Equal(out, b) {
			t.Fatal("directory canonical roundtrip", err)
		}
	})
}
func FuzzFilterHeader(f *testing.F) {
	for _, b := range fuzzCorpus(f) {
		ref, err := Recover(context.Background(), &memoryRangeSource{data: b}, "seed")
		if err != nil {
			f.Fatal(err)
		}
		o := ref.TimelineFilter.Region.Offset
		f.Add(b[o : o+TimelineFilterHeaderBytes])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > TimelineFilterHeaderBytes {
			return
		}
		v, err := UnmarshalFilterHeader(b)
		if err != nil {
			return
		}
		out, err := MarshalFilterHeader(v)
		if err != nil || !bytes.Equal(out, b) {
			t.Fatal("filter header canonical roundtrip", err)
		}
	})
}
func FuzzFilterPage(f *testing.F) {
	for _, b := range fuzzCorpus(f) {
		ref, err := Recover(context.Background(), &memoryRangeSource{data: b}, "seed")
		if err != nil {
			f.Fatal(err)
		}
		fr := *ref.TimelineFilter
		hb, err := MarshalFilterHeader(fr.Header)
		if err != nil {
			f.Fatal(err)
		}
		req, err := PlanFilterPage(fr, ref.MinTimeline)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(hb, ref.MinTimeline, b[req.Offset:req.Offset+req.Length])
	}
	f.Fuzz(func(t *testing.T, hb, timeline, page []byte) {
		if len(hb) > TimelineFilterHeaderBytes || uint64(len(timeline)) > MaxTimelineBytes || len(page) > TimelineFilterPageDataBytes+4 {
			return
		}
		h, err := UnmarshalFilterHeader(hb)
		if err != nil {
			return
		}
		n, err := h.EncodedLength()
		if err != nil {
			return
		}
		ref := testFilterRef(h, 128, n, 128+n+TrailerBytes)
		_, _ = CheckFilterPage(ref, timeline, page)
	})
}

type cappedFuzzSource struct {
	data  []byte
	reads int
	total uint64
}

func (s *cappedFuzzSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}
func (s *cappedFuzzSource) ReadRange(_ context.Context, _ string, off, n int64) ([]byte, error) {
	// Reject before slicing or allocating. Even a future recovery regression
	// cannot turn a fuzz input into an unbounded provider allocation.
	if off < 0 || n < 0 || off > int64(len(s.data)) || n > int64(len(s.data))-off || uint64(n) > MaxDirectoryBytes || s.reads >= 4 || uint64(n) > 2*MaxDirectoryBytes-s.total {
		return nil, fmt.Errorf("fuzz range budget")
	}
	s.reads++
	s.total += uint64(n)
	return s.data[off : off+n : off+n], nil
}
func FuzzRecoverRun(f *testing.F) {
	for _, b := range fuzzCorpus(f) {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 256<<10 {
			return
		}
		ref, err := Recover(context.Background(), &cappedFuzzSource{data: b}, "fuzz")
		if err != nil {
			return
		}
		if err := ref.Validate(); err != nil {
			t.Fatal("recovered invalid reference", err)
		}
		if ref.ObjectSize != uint64(len(b)) {
			t.Fatal("object size mismatch")
		}
	})
}
