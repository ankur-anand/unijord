package lifecycle

import (
	"context"
	"fmt"
	"math"

	"golang.org/x/time/rate"
)

// TokenBucketDeleteLimiter limits physical deletion by object count. Share one
// limiter across reclaimers to enforce a process-wide provider budget.
type TokenBucketDeleteLimiter struct {
	limiter *rate.Limiter
	burst   int
}

// NewTokenBucketDeleteLimiter creates a delete limiter with the supplied
// sustained object rate and burst capacity.
func NewTokenBucketDeleteLimiter(objectsPerSecond float64, burst int) (*TokenBucketDeleteLimiter, error) {
	if objectsPerSecond <= 0 || math.IsNaN(objectsPerSecond) || math.IsInf(objectsPerSecond, 0) {
		return nil, fmt.Errorf("%w: delete rate must be finite and positive", ErrInvalidOptions)
	}
	if burst <= 0 {
		return nil, fmt.Errorf("%w: delete rate burst must be positive", ErrInvalidOptions)
	}
	return &TokenBucketDeleteLimiter{
		limiter: rate.NewLimiter(rate.Limit(objectsPerSecond), burst),
		burst:   burst,
	}, nil
}

// Wait reserves capacity for objects. Requests larger than the configured
// burst are charged in bounded chunks.
func (l *TokenBucketDeleteLimiter) Wait(ctx context.Context, objects int) error {
	if objects < 0 {
		return fmt.Errorf("%w: negative delete object count", ErrInvalidOptions)
	}
	for objects > 0 {
		chunk := min(objects, l.burst)
		if err := l.limiter.WaitN(ctx, chunk); err != nil {
			return err
		}
		objects -= chunk
	}
	return nil
}
