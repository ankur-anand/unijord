package memory_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/memory"
	"github.com/ankur-anand/isledb/blobstore/storetest"
)

func harness(store *memory.Store) storetest.Harness {
	return storetest.Harness{
		Metadata: store, Runs: store, Prefix: "conformance/",
		Reopen: func(*testing.T) (blobstore.MetadataStore, blobstore.RunStore) {
			reopened := store.Reopen()
			return reopened, reopened
		},
		Replace: func(_ *testing.T, key string, body []byte) { store.Replace(key, body) },
		Inject: func(t *testing.T, op storetest.Op, kind storetest.FaultKind) func() int {
			var mu sync.Mutex
			armed, seen := true, 0
			store.SetFault(func(got memory.Op, _ string) memory.Fault {
				mu.Lock()
				defer mu.Unlock()
				if string(got) != string(op) {
					return memory.Fault{}
				}
				seen++
				if !armed {
					return memory.Fault{}
				}
				armed = false
				injected := errors.Join(storetest.ErrInjected, errors.New("memory fault"))
				switch kind {
				case storetest.FaultLostResponse:
					return memory.Fault{After: injected}
				case storetest.FaultCloseBody:
					return memory.Fault{CloseErr: injected}
				default:
					return memory.Fault{Before: injected}
				}
			})
			t.Cleanup(func() { store.SetFault(nil) })
			return func() int {
				mu.Lock()
				defer mu.Unlock()
				return seen
			}
		},
	}
}

func TestMetadataConformance(t *testing.T) { storetest.MetadataSuite(t, harness(memory.New())) }
func TestRunConformance(t *testing.T)      { storetest.RunSuite(t, harness(memory.New())) }
