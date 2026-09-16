package runfile

import "math"

func checkedAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}

func checkedMultiply(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func checkedCeilingDivide(numerator, denominator uint64) (uint64, bool) {
	if denominator == 0 {
		return 0, false
	}
	quotient := numerator / denominator
	if numerator%denominator == 0 {
		return quotient, true
	}
	return checkedAdd(quotient, 1)
}

func checkedAlign(value, alignment uint64) (uint64, bool) {
	if alignment == 0 {
		return 0, false
	}
	remainder := value % alignment
	if remainder == 0 {
		return value, true
	}
	return checkedAdd(value, alignment-remainder)
}

func checkedAlign8(value uint64) (uint64, bool) {
	return checkedAlign(value, RegionAlignment)
}

func checkedRangeEnd(offset, length uint64) (uint64, bool) {
	return checkedAdd(offset, length)
}

func rangeContains(outerOffset, outerLength, innerOffset, innerLength uint64) bool {
	outerEnd, ok := checkedRangeEnd(outerOffset, outerLength)
	if !ok {
		return false
	}
	innerEnd, ok := checkedRangeEnd(innerOffset, innerLength)
	if !ok {
		return false
	}
	return innerOffset >= outerOffset && innerEnd <= outerEnd
}
