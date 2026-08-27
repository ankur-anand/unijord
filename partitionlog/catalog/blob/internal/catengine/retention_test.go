package catengine

import (
	"context"
	"errors"
	"testing"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
)

func TestRetentionRewritesOnlyStraddlingPath(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, pages := buildTestTree(t, config, 8)
	originalObjects := len(pages)

	mutation, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, 5, 1)
	if err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	if got, want := len(mutation.Pages), 3; got != want {
		t.Fatalf("trim page writes = %d, want one leaf and two index pages (%d)", got, want)
	}
	for _, object := range mutation.Pages {
		if _, exists := pages[object.Key]; exists {
			t.Fatalf("retention rewrote existing immutable key %q", object.Key)
		}
		pages[object.Key] = object.Body
	}
	head = mutation.Head
	if head.Header.OldestLSN != 5 || head.Header.NextLSN != 8 || head.Header.SegmentCount != 8 || head.Header.ReachableSegmentCount != 3 {
		t.Fatalf("retained head oldest=%d next=%d segments=%d reachable=%d", head.Header.OldestLSN, head.Header.NextLSN, head.Header.SegmentCount, head.Header.ReachableSegmentCount)
	}
	if head.Header.AppliedRetentionLSN != 5 || head.Header.AppliedRetentionVersion != 1 {
		t.Fatalf("retention marker = (%d,%d)", head.Header.AppliedRetentionLSN, head.Header.AppliedRetentionVersion)
	}
	if len(pages) != originalObjects+3 {
		t.Fatalf("object count = %d, want %d", len(pages), originalObjects+3)
	}

	reader, err := NewReader(config, mapPageSource(pages))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.FindSegment(context.Background(), head, 4); err != nil || found {
		t.Fatalf("FindSegment(4) after retention = found %v error %v", found, err)
	}
	page, err := reader.ListSegments(context.Background(), head, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Segments) != 3 || page.Segments[0].BaseLSN != 5 || page.Segments[2].BaseLSN != 7 {
		t.Fatalf("retained segments = %#v", page.Segments)
	}
}

func TestFullRetentionKeepsTipAndAllowsNextAppend(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, pages := buildTestTree(t, config, 8)

	mutation, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, head.Header.NextLSN, 1)
	if err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	head = mutation.Head
	if !head.HasLastSegment() || head.LastSegment.LastLSN != 7 {
		t.Fatalf("fully retained head lost append tip: %#v", head.LastSegment)
	}
	if head.Header.OldestLSN != 8 || head.Header.NextLSN != 8 || head.Header.SegmentCount != 8 || head.Header.ReachableSegmentCount != 0 || len(headRoots(head)) != 0 {
		t.Fatalf("fully retained head still exposes history: %#v", head)
	}
	reader, err := NewReader(config, mapPageSource(pages))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.FindSegment(context.Background(), head, 7); err != nil || found {
		t.Fatalf("FindSegment(7) = found %v error %v, want retained", found, err)
	}

	appendMutation, err := Append(config, head, testSegment(config, head, 8))
	if err != nil {
		t.Fatalf("Append after full retention error = %v", err)
	}
	head = appendMutation.Head
	if head.Header.OldestLSN != 8 || head.Header.NextLSN != 9 || head.Header.SegmentCount != 9 || head.Header.ReachableSegmentCount != 1 || len(head.Active) != 1 {
		t.Fatalf("head after resumed append = %#v", head)
	}
}

func TestRetentionRejectsRegressionsAndAcceptsExactRetry(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, pages := buildTestTree(t, config, 2)
	mutation, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	head = mutation.Head

	retry, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, 1, 3)
	if err != nil {
		t.Fatalf("exact retry error = %v", err)
	}
	if retry.Head.Header.Generation != head.Header.Generation || len(retry.Pages) != 0 {
		t.Fatalf("exact retry mutated state: %#v", retry)
	}
	if _, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, 1, 2); !errors.Is(err, csession.ErrRetentionRegression) {
		t.Fatalf("policy regression error = %v", err)
	}
	if _, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, 0, 4); !errors.Is(err, csession.ErrRetentionRegression) {
		t.Fatalf("LSN regression error = %v", err)
	}
}

func TestApplyRetentionRejectsExhaustedGeneration(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, pages := buildTestTree(t, config, 1)
	head.Header.Generation = ^uint64(0)

	if _, err := ApplyRetention(context.Background(), config, mapPageSource(pages), head, head.Header.OldestLSN, 1); !errors.Is(err, csession.ErrGenerationExhausted) {
		t.Fatalf("ApplyRetention() error = %v, want ErrGenerationExhausted", err)
	}
}

func buildTestTree(t *testing.T, config Config, count uint64) (head catformat.Head, pages map[string][]byte) {
	t.Helper()
	var err error
	head, _, err = NewHead(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	takeover, err := Takeover(config, head, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	head = takeover.Head
	pages = make(map[string][]byte)
	for lsn := uint64(0); lsn < count; lsn++ {
		mutation, err := Append(config, head, testSegment(config, head, lsn))
		if err != nil {
			t.Fatalf("Append(%d) error = %v", lsn, err)
		}
		for _, object := range mutation.Pages {
			if _, exists := pages[object.Key]; exists {
				t.Fatalf("duplicate immutable page key %q", object.Key)
			}
			pages[object.Key] = object.Body
		}
		head = mutation.Head
	}
	return head, pages
}
