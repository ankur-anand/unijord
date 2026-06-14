// Package cache contains SegmentStore decorators for immutable segment
// byte ranges.
//
// It is a read-side optimization only. Segment objects are immutable, so cache
// entries do not need invalidation; eviction is controlled by a byte budget.
package cache
