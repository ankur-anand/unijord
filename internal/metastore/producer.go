package metastore

import (
	"context"
	"fmt"
)

// ProducerKey is namespace-scoped and independent of shard-writer ownership.
// Lifecycle calls assume the service has already authorized namespace access.
type ProducerKey struct {
	Namespace Namespace
	ID        ProducerID
}

type ProducerFence struct {
	Key           ProducerKey
	IncarnationID ProducerIncarnationID
	Epoch         uint64
}

type ProducerStatus uint8

const (
	ProducerOpen ProducerStatus = iota + 1
	ProducerClosed
)

// ProducerState is a current authority snapshot, not an operation receipt.
// NextSequence survives both close and resume. Only a future direct commit
// transaction may advance it together with its durable operation result.
type ProducerState struct {
	Fence        ProducerFence
	NextSequence uint64
	Status       ProducerStatus
}

type OpenProducerRequest struct {
	Key           ProducerKey
	IncarnationID ProducerIncarnationID
}

type ResumeProducerRequest struct {
	Key           ProducerKey
	ExpectedEpoch uint64
	IncarnationID ProducerIncarnationID
}

type ProducerLifecycle interface {
	// OpenProducer creates epoch 1 at sequence 0. An exact initial-incarnation
	// retry returns current state without reopening a closed producer.
	OpenProducer(context.Context, OpenProducerRequest) (ProducerState, error)
	// ResumeProducer explicitly replaces a client incarnation. It compares
	// ExpectedEpoch, installs a fresh incarnation and increments the epoch.
	// A retry of that exact transition returns current state without mutation.
	ResumeProducer(context.Context, ResumeProducerRequest) (ProducerState, error)
	GetProducerState(context.Context, ProducerKey) (ProducerState, error)
	// CloseProducer closes only this epoch, not its timelines. A new incarnation
	// may explicitly resume it; a delayed close cannot close the replacement.
	CloseProducer(context.Context, ProducerFence) (ProducerState, error)
}

func ValidateProducerKey(key ProducerKey) error {
	if err := ValidateNamespace(key.Namespace); err != nil {
		return err
	}
	if isZero128(key.ID) {
		return fmt.Errorf("%w: zero producer ID", ErrInvalidRequest)
	}
	return nil
}

func ValidateProducerFence(fence ProducerFence) error {
	if err := ValidateProducerKey(fence.Key); err != nil {
		return err
	}
	if isZero128(fence.IncarnationID) || fence.Epoch == 0 {
		return fmt.Errorf("%w: incomplete producer fence", ErrInvalidRequest)
	}
	return nil
}

func ValidateProducerState(state ProducerState) error {
	if err := ValidateProducerFence(state.Fence); err != nil {
		return fmt.Errorf("%w: invalid producer fence: %v", ErrCorrupt, err)
	}
	if state.Status != ProducerOpen && state.Status != ProducerClosed {
		return fmt.Errorf("%w: invalid producer status", ErrCorrupt)
	}
	return nil
}

func ValidateOpenProducer(request OpenProducerRequest) error {
	return ValidateProducerFence(ProducerFence{
		Key: request.Key, IncarnationID: request.IncarnationID, Epoch: 1,
	})
}

func ValidateResumeProducer(request ResumeProducerRequest) error {
	return ValidateProducerFence(ProducerFence{
		Key: request.Key, IncarnationID: request.IncarnationID, Epoch: request.ExpectedEpoch,
	})
}

func SameProducerFence(a, b ProducerFence) bool {
	return a.Key.Namespace.Equal(b.Key.Namespace) && a.Key.ID == b.Key.ID &&
		a.IncarnationID == b.IncarnationID && a.Epoch == b.Epoch
}
