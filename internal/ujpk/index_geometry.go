package ujpk

import "fmt"

// selectIndexPageBytes chooses a deterministic writer policy. A positive
// override is preserved exactly. Auto mode compares power-of-two targets so
// implementations do not need floating-point agreement to emit equal packs.
func selectIndexPageBytes(extentCount, dataPageCount, override int) (int, error) {
	if extentCount <= 0 || dataPageCount <= 0 {
		return 0, fmt.Errorf("%w: index geometry extents=%d data_pages=%d", ErrInvalidPack, extentCount, dataPageCount)
	}
	if override != AutoIndexPageBytes {
		_, _, fits := indexReadGeometry(extentCount, dataPageCount, override)
		if !fits {
			return 0, fmt.Errorf("%w: index root exceeds %d bytes", ErrRecordTooLarge, MaxIndexRootBytes)
		}
		return override, nil
	}

	bestTarget := 0
	bestCost := ^uint64(0)
	for target := MinAutoIndexPageBytes; target <= MaxIndexPageBytes; target <<= 1 {
		rootBytes, selectedPageBytes, fits := indexReadGeometry(extentCount, dataPageCount, target)
		if fits {
			cost := rootBytes + selectedPageBytes
			if cost < bestCost {
				bestTarget = target
				bestCost = cost
			}
		}
		if target == MaxIndexPageBytes {
			break
		}
	}
	if bestTarget == 0 {
		return 0, fmt.Errorf("%w: index root exceeds %d bytes", ErrRecordTooLarge, MaxIndexRootBytes)
	}
	return bestTarget, nil
}

// indexReadGeometry returns the root bytes and the largest selected index-page
// read for a point lookup. The data-page table is included because it shares
// the same bounded root, although it is constant across index-page choices.
func indexReadGeometry(extentCount, dataPageCount, targetBytes int) (rootBytes, selectedPageBytes uint64, fits bool) {
	if extentCount <= 0 || dataPageCount <= 0 ||
		targetBytes < IndexPagePreambleSize+TimelineIndexEntrySize || targetBytes > MaxIndexPageBytes {
		return 0, 0, false
	}
	entriesPerPage := (targetBytes - IndexPagePreambleSize) / TimelineIndexEntrySize
	indexPageCount := (extentCount + entriesPerPage - 1) / entriesPerPage
	selectedEntries := min(extentCount, entriesPerPage)
	rootBytes = uint64(IndexRootPreambleSize) +
		uint64(dataPageCount)*uint64(DataPageTableEntrySize) +
		uint64(indexPageCount)*uint64(IndexRootEntrySize)
	selectedPageBytes = uint64(IndexPagePreambleSize) +
		uint64(selectedEntries)*uint64(TimelineIndexEntrySize)
	return rootBytes, selectedPageBytes, rootBytes <= MaxIndexRootBytes
}
