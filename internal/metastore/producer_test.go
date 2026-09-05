package metastore_test

import (
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
)

func TestProducerLifecycleValidation(t *testing.T) {
	key := metastore.ProducerKey{Namespace: metastore.CopyNamespace([]byte("tenant")), ID: metastore.ProducerID{1}}
	fence := metastore.ProducerFence{Key: key, IncarnationID: metastore.ProducerIncarnationID{2}, Epoch: 1}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"zero-key", metastore.ValidateProducerKey(metastore.ProducerKey{})},
		{"zero-id", metastore.ValidateProducerKey(metastore.ProducerKey{Namespace: key.Namespace})},
		{"zero-open-incarnation", metastore.ValidateOpenProducer(metastore.OpenProducerRequest{Key: key})},
		{"zero-resume-epoch", metastore.ValidateResumeProducer(metastore.ResumeProducerRequest{Key: key, IncarnationID: fence.IncarnationID})},
		{"zero-fence-epoch", metastore.ValidateProducerFence(metastore.ProducerFence{Key: key, IncarnationID: fence.IncarnationID})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, metastore.ErrInvalidRequest) {
				t.Fatalf("validation error=%v", tc.err)
			}
		})
	}
	if err := metastore.ValidateOpenProducer(metastore.OpenProducerRequest{Key: key, IncarnationID: fence.IncarnationID}); err != nil {
		t.Fatal(err)
	}
	for _, status := range []metastore.ProducerStatus{metastore.ProducerOpen, metastore.ProducerClosed} {
		if err := metastore.ValidateProducerState(metastore.ProducerState{Fence: fence, Status: status}); err != nil {
			t.Fatal(err)
		}
	}
	if err := metastore.ValidateProducerState(metastore.ProducerState{Fence: fence}); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("invalid durable status: %v", err)
	}
	if !metastore.SameProducerFence(fence, fence) {
		t.Fatal("equal fence differs")
	}
	other := fence
	other.Key.Namespace = metastore.CopyNamespace([]byte("another-tenant"))
	if metastore.SameProducerFence(fence, other) {
		t.Fatal("producer authority leaked across namespaces")
	}
}
