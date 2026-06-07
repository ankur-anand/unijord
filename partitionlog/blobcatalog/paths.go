package blobcatalog

import (
	"fmt"
	"strings"
)

const DefaultPrefix = "catalog"

func HeadPath(prefix string, partition uint32) string {
	return fmt.Sprintf("%s/p%08d/head.json", normalizePrefix(prefix), partition)
}

func PagePrefix(prefix string, partition uint32) string {
	return fmt.Sprintf("%s/p%08d/pages/", normalizePrefix(prefix), partition)
}

func LeafPagePath(prefix string, partition uint32, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/p%08d/pages/l00/leaf-%020d-%020d-%020d-%s.json",
		normalizePrefix(prefix), partition, seqLo, seqHi, generation, pageID,
	)
}

func IndexPagePath(prefix string, partition uint32, level uint8, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/p%08d/pages/l%02d/index-l%02d-%020d-%020d-%020d-%s.json",
		normalizePrefix(prefix), partition, level, level, seqLo, seqHi, generation, pageID,
	)
}

func normalizePrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return DefaultPrefix
	}
	return prefix
}
