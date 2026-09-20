package isledb

import (
	"path"
	"strconv"

	"github.com/ankur-anand/isledb/blobstore"
)

const defaultReclaimDeleteConcurrency = 4

func storeKey(store *blobstore.Store, parts ...string) string {
	if store == nil {
		return path.Join(parts...)
	}
	all := make([]string, 0, len(parts)+1)
	if prefix := store.Prefix(); prefix != "" {
		all = append(all, prefix)
	}
	all = append(all, parts...)
	return path.Join(all...)
}

func matchTokenFromAttrs(attrs blobstore.Attributes) string {
	if attrs.Generation != 0 {
		return fmtGeneration(attrs.Generation)
	}
	return attrs.ETag
}

func fmtGeneration(g int64) string {
	return strconv.FormatInt(g, 10)
}
