package manifest

import (
	"bytes"
	"testing"
)

func FuzzRunManifestDecode(f *testing.F) {
	m := testRunManifest(testRun(1, 0))
	cp, _ := EncodeRunCheckpoint(m)
	e := RunLogEntry{Op: RunLogAdd, Revision: 2, NextSequence: 2, AddRuns: []RunMeta{testRun(1, 0)}}
	p, _ := EncodeRunPage(&RunPage{SeqLo: 2, SeqHi: 2, Count: 1, Entries: []RunLogEntry{e}})
	l, _ := EncodeRunLogEntry(&e)
	f.Add(byte(1), cp[48:])
	f.Add(byte(2), p[48:])
	f.Add(byte(3), l[48:])
	f.Add(byte(1), []byte(`{"L0SSTs":[],"L0Runs":[]}`))
	f.Fuzz(func(t *testing.T, kind byte, payload []byte) {
		if len(payload) > 64<<10 {
			return
		}
		// Recompute the envelope hash so mutated data reaches the bounded
		// inner parser. Maximum possible admitted metadata is <164 runs.
		data := runEnvelope(payload, kind)
		var encoded []byte
		var err error
		switch kind {
		case 1:
			m, e := DecodeRunCheckpoint(data)
			if e != nil {
				return
			}
			encoded, err = EncodeRunCheckpoint(m)
		case 2:
			p, e := DecodeRunPage(data)
			if e != nil {
				return
			}
			encoded, err = EncodeRunPage(p)
		case 3:
			e, x := DecodeRunLogEntry(data)
			if x != nil {
				return
			}
			encoded, err = EncodeRunLogEntry(e)
		default:
			if _, e := DecodeRunCheckpoint(data); e == nil {
				t.Fatal("unknown kind")
			}
			return
		}
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatalf("noncanonical accepted bytes: %v", err)
		}
	})
}

func FuzzRunManifestReplay(f *testing.F) {
	e := RunLogEntry{Op: RunLogAdd, Revision: 2, NextSequence: 2, AddRuns: []RunMeta{testRun(1, 0)}}
	data, _ := EncodeRunLogEntry(&e)
	f.Add(data[48:])
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 64<<10 {
			return
		}
		e, err := DecodeRunLogEntry(runEnvelope(payload, runLogKind))
		if err != nil {
			return
		}
		m := testRunManifest()
		m.NextSequence = 1
		before, _ := EncodeRunCheckpoint(m)
		out, err := ApplyRunLogEntry(m, e)
		after, _ := EncodeRunCheckpoint(m)
		if !bytes.Equal(before, after) {
			t.Fatal("mutated input")
		}
		if err == nil {
			if err := out.Validate(); err != nil {
				t.Fatal(err)
			}
			b, err := EncodeRunCheckpoint(out)
			if err != nil {
				t.Fatal(err)
			}
			round, err := DecodeRunCheckpoint(b)
			if err != nil || !out.Equal(round) {
				t.Fatal("replayed invalid state", err)
			}
		}
	})
}
