package raft

import (
	"time"
)

const (
	heartbeatInterval   = 500 * time.Millisecond
	idleTimeout         = 1 * time.Second
	electionTimeout     = 2 * time.Second
	electionMaxWaitTime = 1 * time.Second
	tickDuration        = 10 * time.Millisecond
)

type RaftRole int

const (
	RaftRoleLeader RaftRole = iota + 1
	RaftRoleFollower
	RaftRoleCandidate
	RaftRoleDead
)

func (r RaftRole) String() string {
	return map[RaftRole]string{
		RaftRoleLeader:    "Leader",
		RaftRoleFollower:  "Follower",
		RaftRoleCandidate: "Candidate",
		RaftRoleDead:      "Dead",
	}[r]
}
