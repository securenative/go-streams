package go_streams

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExample_FindPrimes(t *testing.T) {
	// Finding all primes up to 1000 using bare streams

	// Create a source of sequential integers: (e.g: 1,2,3,4,5,...,1000)
	source := NewSequentialIntegerSource(1000, 250*time.Microsecond)

	// Create an array sink: (i.e each item will be appended to the array)
	array := NewArraySink()

	// Process messages in batches of 12 entries or 200ms from last batch
	processor := NewBufferedProcessor(12, 256*time.Millisecond)

	// Make an error channel
	errs := make(ErrorChannel, 1000)

	s := NewStream(source). // Create a new stream from the integers source
				Filter(onlyPrimes). // Filter only numbers that are onlyPrimes
				Sink(array)         // Dump the primes to array

	// Blocks until source return EOF error (i.e source is done loading data)
	s.Process(processor, errs)

	assert.NotEmpty(t, array.Array())
	assert.EqualValues(t, 168, len(array.array))
	assert.EqualValues(t, []interface{}{
		2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
		101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199,
		211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293,
		307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397,
		401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499,
		503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599,
		601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691,
		701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797,
		809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887,
		907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997,
	}, array.array)
}

func TestExample_DedupNumbers(t *testing.T) {
	source := NewSequentialIntegerSource(1000, 250*time.Microsecond)
	mapSink := NewReplacingMapSink()
	processor := NewBufferedProcessor(23, 256*time.Millisecond)
	errs := make(ErrorChannel, 1000)

	mapSink.SetKeyExtractor(func(entry Entry) string {
		return fmt.Sprintf("%d", entry.Value.(int))
	})

	s := NewStream(source).
		Map(mod(12)).
		Sink(mapSink)

	s.Process(processor, errs)

	assert.NotEmpty(t, mapSink.Map())
	assert.EqualValues(t, 12, len(mapSink.Map()))
}

func mod(num int) MapFunc {
	return func(entry interface{}) interface{} {
		return entry.(int) % num
	}
}

func onlyPrimes(entry interface{}) bool {
	num := entry.(int) // panics will be recovered

	// 0,1 and even numbers (except from 2) can be filtered directly
	if num <= 1 || (num > 2 && num%2 == 0) {
		return false
	}

	for i := 2; i < num; i++ {
		if num%i == 0 {
			return false
		}
	}
	return true
}
