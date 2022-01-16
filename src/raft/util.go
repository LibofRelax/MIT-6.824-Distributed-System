package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func waitRandomTime(max time.Duration) {
	n := rand.Intn(1)
	time.Sleep(time.Duration(n) * max)
}

func majority(n int) int {
	return n/2 + 1
}
