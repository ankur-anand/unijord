package postgres

import (
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/metastore"
)

type producerTransition func(metastore.ProducerState) (metastore.ProducerState, error)

func planProducerOpen(current metastore.ProducerState, request metastore.OpenProducerRequest) (metastore.ProducerState, error) {
	if current.Fence.Epoch != 1 || current.Fence.IncarnationID != request.IncarnationID {
		return metastore.ProducerState{}, fmt.Errorf("%w: producer already has another incarnation", metastore.ErrConflict)
	}
	return current, nil // An open retry must not undo a subsequent close.
}

func planProducerResume(current metastore.ProducerState, request metastore.ResumeProducerRequest) (metastore.ProducerState, error) {
	if current.Fence.IncarnationID == request.IncarnationID && request.ExpectedEpoch != math.MaxUint64 &&
		current.Fence.Epoch == request.ExpectedEpoch+1 {
		return current, nil // Exact takeover retry, even if this epoch has closed.
	}
	if current.Fence.Epoch != request.ExpectedEpoch {
		return metastore.ProducerState{}, metastore.ErrStaleProducer
	}
	if current.Fence.IncarnationID == request.IncarnationID {
		return metastore.ProducerState{}, fmt.Errorf("%w: resume requires a new incarnation", metastore.ErrConflict)
	}
	if current.Fence.Epoch == math.MaxUint64 {
		return metastore.ProducerState{}, fmt.Errorf("%w: producer epoch exhausted", metastore.ErrConflict)
	}
	current.Fence.Epoch++
	current.Fence.IncarnationID = request.IncarnationID
	current.Status = metastore.ProducerOpen
	return current, nil
}

func planProducerClose(current metastore.ProducerState, fence metastore.ProducerFence) (metastore.ProducerState, error) {
	if !metastore.SameProducerFence(current.Fence, fence) {
		return metastore.ProducerState{}, metastore.ErrStaleProducer
	}
	current.Status = metastore.ProducerClosed
	return current, nil
}
