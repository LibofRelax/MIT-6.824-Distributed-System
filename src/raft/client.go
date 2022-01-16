package raft

import (
	"context"
	"fmt"
	"time"

	"distrubuted_system/labrpc"
)

const (
	rpcTimeout = 1 * time.Second
)

var (
	RPCTimeoutErr = fmt.Errorf("rpc call timeout")
	CallErr       = fmt.Errorf("rpc call error")
)

type AppendEntriesReq struct {
	Entries []*LogEntry
}

type AppendEntriesRsp struct {
}

func AppendEntries(ctx context.Context, peer *labrpc.ClientEnd, req *AppendEntriesReq) (*AppendEntriesRsp, error) {
	c := make(chan *AppendEntriesRsp, 1)
	go func() {
		rsp := &AppendEntriesRsp{}
		_ = peer.Call("Raft.AppendEntries", req, rsp)
		c <- rsp
	}()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, RPCTimeoutErr

		case rsp := <-c:
			if rsp != nil {
				return rsp, nil
			} else {
				return nil, CallErr
			}
		}
	}
}

type RequestVoteReq struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateIdx int
	LastLogIdx   int
	LastLogTerm  int
}

type RequestVoteRsp struct {
	// Your data here (2A).
	Term  int
	Grant bool
}

func RequestVote(ctx context.Context, peer *labrpc.ClientEnd, req *RequestVoteReq) (*RequestVoteRsp, error) {
	c := make(chan *RequestVoteRsp, 1)
	go func() {
		rsp := &RequestVoteRsp{}
		_ = peer.Call("Raft.RequestVote", req, rsp)
		c <- rsp
	}()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, RPCTimeoutErr

		case rsp := <-c:
			if rsp != nil {
				return rsp, nil
			} else {
				return nil, CallErr
			}
		}
	}
}
