package runfile

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidRun reports invalid caller or builder input.
	ErrInvalidRun = errors.New("runfile: invalid run")
	// ErrCorruptRun reports malformed or inconsistent persisted bytes.
	ErrCorruptRun = errors.New("runfile: corrupt run")
	// ErrUnsupportedRunVersion reports valid framing with an unsupported version
	// or algorithm.
	ErrUnsupportedRunVersion = errors.New("runfile: unsupported run version")
	// ErrRunTooLarge reports a configured or format-size limit violation.
	ErrRunTooLarge = errors.New("runfile: run too large")
	// ErrFilterUnavailable reports that an optional filter could not be fetched.
	ErrFilterUnavailable = errors.New("runfile: filter unavailable")
	// ErrVerificationResource reports scratch, descriptor, or other local
	// resource exhaustion while running offline complete verification.
	ErrVerificationResource = errors.New("runfile: verification resource failure")
	// ErrObjectIdentityChanged reports that a conditional object read observed
	// a different immutable provider generation.
	ErrObjectIdentityChanged = errors.New("runfile: object identity changed")
)

func invalidRunf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRun, fmt.Sprintf(format, args...))
}

func corruptRunf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruptRun, fmt.Sprintf(format, args...))
}

func unsupportedRunf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedRunVersion, fmt.Sprintf(format, args...))
}

func runTooLargef(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRunTooLarge, fmt.Sprintf(format, args...))
}
