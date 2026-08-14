package s3

import (
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/blobstore"
	"github.com/aws/smithy-go"
)

func TestMapErrorPreconditionFailure(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"PreconditionFailed", "ConditionalRequestConflict"} {
		t.Run(code, func(t *testing.T) {
			err := mapError(&smithy.GenericAPIError{Code: code, Message: "conditional write lost"})
			if !errors.Is(err, blobstore.ErrImmutableConflict) {
				t.Fatalf("mapError() = %v, want %v", err, blobstore.ErrImmutableConflict)
			}
		})
	}
}
