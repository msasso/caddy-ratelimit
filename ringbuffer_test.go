// Copyright 2023 Matthew Holt

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//  http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package caddyrl

import (
	"testing"
	"time"
)

func TestCount(t *testing.T) {
	initTime()

	var zeroTime time.Time
	bufSize := 10
	rb := newRingBufferRateLimiter(bufSize, time.Duration(bufSize)*time.Second, 0)
	startTime := now()

	count, oldest := rb.Count(now())
	if count != 0 {
		t.Fatalf("count should be 0 for empty ring buffer")
	}
	if oldest != zeroTime {
		t.Fatalf("oldest event should be zero value for empty ring buffer")
	}

	// Fill the buffer with events spaced out by 1s
	for i := 0; i < bufSize; i++ {
		advanceTime(i)
		if when := rb.When(); when != 0 {
			t.Fatalf("empty ring buffer should allow events")
		}

		count, oldest = rb.Count(now())
		if count != i+1 {
			t.Fatalf("count %d is wrong", count)
		}
		if oldest != startTime {
			t.Fatalf("oldest time %+v is wrong", oldest)
		}
	}

	if when := rb.When(); when != time.Second {
		t.Fatal("full ring buffer should forbid events")
	}

	count, oldest = rb.Count(now())
	if count != bufSize {
		t.Fatalf("count %d is wrong", count)
	}
	if oldest != startTime {
		t.Fatalf("oldest time %+v is wrong", oldest)
	}

	// Advance time by half the window. Only half the events should be counted,
	// and the oldest event should be updated.
	advanceTime(bufSize + bufSize/2)

	count, oldest = rb.Count(now())
	if count != bufSize/2 {
		t.Fatalf("count %d is wrong", count)
	}
	if oldest != startTime.Add(time.Duration(bufSize/2)*time.Second) {
		t.Fatalf("oldest time %+v is wrong", oldest)
	}

	// Advance by the whole window. There should now be no events in the window
	advanceTime(2 * bufSize)

	count, oldest = rb.Count(now())
	if count != 0 {
		t.Fatalf("count %d is wrong", count)
	}
	if oldest != zeroTime {
		t.Fatalf("oldest time %+v is wrong", oldest)
	}
}

func TestLeakyBucket(t *testing.T) {
	initTime()

	// bucket capacity of 3 events, leaking one event every 10s
	// (i.e. max_events 3 per window 30s with burst 3)
	emission := 10 * time.Second
	rb := newRingBufferRateLimiter(3, 3*emission, emission)

	// a full bucket absorbs a burst of 3, then rejects
	for i := 0; i < 3; i++ {
		if when := rb.When(); when != 0 {
			t.Fatalf("burst event %d should be allowed, got wait of %v", i, when)
		}
	}
	if when := rb.When(); when != emission {
		t.Fatalf("empty bucket should require a wait of one emission interval, got %v", when)
	}

	// after one emission interval, exactly one event has leaked out
	advanceTime(10)
	if when := rb.When(); when != 0 {
		t.Fatalf("one event should be allowed after leak interval, got wait of %v", when)
	}
	if when := rb.When(); when != emission {
		t.Fatalf("second event should wait one emission interval, got %v", when)
	}

	// 25s later, two more events have leaked out (2.5 rounds down)
	advanceTime(35)
	for i := 0; i < 2; i++ {
		if when := rb.When(); when != 0 {
			t.Fatalf("drained event %d should be allowed, got wait of %v", i, when)
		}
	}
	if when := rb.When(); when != emission/2 {
		t.Fatalf("expected wait of %v for next event, got %v", emission/2, when)
	}

	// a long idle period refills the bucket, but only up to its capacity
	advanceTime(1000)
	for i := 0; i < 3; i++ {
		if when := rb.When(); when != 0 {
			t.Fatalf("event %d after idle should be allowed, got wait of %v", i, when)
		}
	}
	if when := rb.When(); when != emission {
		t.Fatalf("bucket should not refill beyond capacity; expected wait %v, got %v", emission, when)
	}
}
