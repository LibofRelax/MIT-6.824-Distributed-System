package raft

import (
	"time"
)

const (
	heartbeatInterval = 500 * time.Millisecond
	idleTimeout       = 1 * time.Second
	heartbeatTimeout  = 2 * time.Second
	electionTimeout   = 5 * time.Second
	tickDuration      = 10 * time.Millisecond
)

const (
	StateLeader int = iota + 1
	StateFollower
	StateCandidate
	StateKilled
)
