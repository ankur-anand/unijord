package blobcatalog

import "fmt"

func validatePageRef(ref pageRef, level uint8) error {
	if ref.Level != level {
		return fmt.Errorf("%w: page ref level=%d want=%d", ErrCorruptCatalog, ref.Level, level)
	}
	if ref.SeqLo > ref.SeqHi {
		return fmt.Errorf("%w: page ref seq_lo=%d seq_hi=%d", ErrCorruptCatalog, ref.SeqLo, ref.SeqHi)
	}
	if ref.Generation == 0 || ref.PageID == "" || ref.Path == "" || ref.Count <= 0 {
		return fmt.Errorf("%w: incomplete page ref", ErrCorruptCatalog)
	}
	return nil
}
