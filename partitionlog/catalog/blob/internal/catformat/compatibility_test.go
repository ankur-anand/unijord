package catformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type corpusManifest struct {
	Format   string          `json:"format"`
	Version  uint16          `json:"version"`
	Fixtures []corpusFixture `json:"fixtures"`
}

type corpusFixture struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	File   string `json:"file"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
	PageID string `json:"page_id"`
}

func TestCompatibilityCorpus(t *testing.T) {
	root := filepath.Join("testdata", "v1")
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest corpusManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Format != "catformat" || manifest.Version != Version {
		t.Fatalf("manifest identifies %q v%d, want catformat v%d", manifest.Format, manifest.Version, Version)
	}
	if len(manifest.Fixtures) == 0 {
		t.Fatal("compatibility corpus is empty")
	}

	for _, fixture := range manifest.Fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			encoded, err := os.ReadFile(filepath.Join(root, fixture.File))
			if err != nil {
				t.Fatal(err)
			}
			if len(encoded) != fixture.Size {
				t.Fatalf("size = %d, manifest = %d", len(encoded), fixture.Size)
			}
			sum := sha256.Sum256(encoded)
			if got := hex.EncodeToString(sum[:]); got != fixture.SHA256 {
				t.Fatalf("sha256 = %s, manifest = %s", got, fixture.SHA256)
			}

			var remarshal []byte
			switch fixture.Kind {
			case "head":
				head, err := ParseHead(encoded)
				if err != nil {
					t.Fatalf("ParseHead() error = %v", err)
				}
				remarshal, err = MarshalHead(head)
				if err != nil {
					t.Fatalf("MarshalHead() error = %v", err)
				}
			case "leaf_page":
				page, id, err := ParseLeafPage(encoded)
				if err != nil {
					t.Fatalf("ParseLeafPage() error = %v", err)
				}
				assertCorpusPageID(t, id, fixture.PageID)
				remarshal, _, err = MarshalLeafPage(page)
				if err != nil {
					t.Fatalf("MarshalLeafPage() error = %v", err)
				}
			case "index_page":
				page, id, err := ParseIndexPage(encoded)
				if err != nil {
					t.Fatalf("ParseIndexPage() error = %v", err)
				}
				assertCorpusPageID(t, id, fixture.PageID)
				remarshal, _, err = MarshalIndexPage(page)
				if err != nil {
					t.Fatalf("MarshalIndexPage() error = %v", err)
				}
			default:
				t.Fatalf("unknown fixture kind %q", fixture.Kind)
			}
			if !bytes.Equal(remarshal, encoded) {
				t.Fatal("parse and re-marshal changed the frozen bytes")
			}
		})
	}
}

func assertCorpusPageID(t *testing.T, id [16]byte, encoded string) {
	t.Helper()
	want, err := hex.DecodeString(encoded)
	if err != nil || len(want) != len(id) {
		t.Fatalf("invalid page_id %q in manifest: %v", encoded, err)
	}
	if !bytes.Equal(id[:], want) {
		t.Fatalf("page ID = %x, manifest = %s", id, encoded)
	}
}

func ExampleParseHead() {
	encoded, err := os.ReadFile("testdata/v1/empty-head-crc32c.plc")
	if err != nil {
		panic(err)
	}
	head, err := ParseHead(encoded)
	if err != nil {
		panic(err)
	}
	fmt.Printf("partition=%d next_lsn=%d\n", head.Header.Partition, head.Header.NextLSN)
	// Output: partition=7 next_lsn=0
}
