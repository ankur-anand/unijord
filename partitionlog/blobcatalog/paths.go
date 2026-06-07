package blobcatalog

import (
	"fmt"
	"strings"
)

const DefaultPrefix = "catalog"

func HeadPath(prefix string, partition uint32) string {
	return fmt.Sprintf("%s/p%08d/head.json", normalizePrefix(prefix), partition)
}

func normalizePrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return DefaultPrefix
	}
	return prefix
}
