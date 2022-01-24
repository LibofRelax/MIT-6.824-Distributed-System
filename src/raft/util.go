package raft

import (
	"math/rand"
	"time"
)

func randomDuration(max time.Duration) time.Duration {
	n := rand.Float32()
	return time.Duration(n * float32(max))
}

func majorityOf(n int) int {
	return n/2 + 1
}
