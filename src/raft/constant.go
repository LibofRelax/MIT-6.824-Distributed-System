package raft

import (
	"fmt"
	"time"
)

const (
	heartbeatInterval    = 500 * time.Millisecond
	idleTimeout          = 1 * time.Second
	heartbeatTimeout     = 2 * time.Second
	electionTimeout      = 5 * time.Second
	appendEntriesTimeout = 2 * time.Second

	rpcTimeout = 1 * time.Second
)

var (
	timeoutError = fmt.Errorf("time out")
	rpcError     = fmt.Errorf("rpc call error")
)

type raftState int

const (
	leader raftState = iota + 1
	follower
	candidate
	killed
)
