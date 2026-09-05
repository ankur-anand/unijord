package postgres

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/ankur-anand/unijord/internal/metastore"
)

type shardIdentity struct {
	namespaceHash [32]byte
	shard         uint32
}

type claimItem struct {
	identity shardIdentity
	key      metastore.ShardKey
	input    int
}

type namespaceClaim struct {
	hash      [32]byte
	namespace metastore.Namespace
}

func prepareClaim(shards []metastore.ShardKey) ([]claimItem, []namespaceClaim, error) {
	items := make([]claimItem, len(shards))
	byNamespace := make(map[[32]byte]metastore.Namespace, len(shards))
	for i, key := range shards {
		hash := key.Namespace.Hash()
		if prior, exists := byNamespace[hash]; exists && !prior.Equal(key.Namespace) {
			return nil, nil, fmt.Errorf("%w: namespace digest collision", metastore.ErrCorrupt)
		}
		byNamespace[hash] = key.Namespace
		items[i] = claimItem{identity: shardIdentity{namespaceHash: hash, shard: key.Shard}, key: key, input: i}
	}
	slices.SortFunc(items, func(a, b claimItem) int {
		if order := bytes.Compare(a.identity.namespaceHash[:], b.identity.namespaceHash[:]); order != 0 {
			return order
		}
		return compareUint32(a.identity.shard, b.identity.shard)
	})
	namespaces := make([]namespaceClaim, 0, len(byNamespace))
	for hash, namespace := range byNamespace {
		namespaces = append(namespaces, namespaceClaim{hash: hash, namespace: namespace})
	}
	slices.SortFunc(namespaces, func(a, b namespaceClaim) int {
		return bytes.Compare(a.hash[:], b.hash[:])
	})
	return items, namespaces, nil
}

func claimArrays(items []claimItem) ([][]byte, []int64) {
	hashes := make([][]byte, len(items))
	shards := make([]int64, len(items))
	for i := range items {
		hashes[i] = items[i].identity.namespaceHash[:]
		shards[i] = int64(items[i].identity.shard)
	}
	return hashes, shards
}

func compareUint32(a, b uint32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
