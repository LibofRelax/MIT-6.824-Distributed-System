package client

import (
	"github.com/pkg/errors"

	"distrubuted_system/labrpc"
	"distrubuted_system/raft"
)

type AppendEntriesReq struct {
	Entries []*raft.LogEntry
}

type AppendEntriesRsp struct {
}

func AppendEntries(peer *labrpc.ClientEnd, req *AppendEntriesReq) (*AppendEntriesRsp, error) {
	rsp := &AppendEntriesRsp{}

	if ok := peer.Call("Raft.AppendEntries", req, rsp); !ok {
		return nil, errors.New("call failed")
	}

	return rsp, nil
}

type RequestVoteReq struct {
	// Your data here (2A, 2B).
}

type RequestVoteRsp struct {
	// Your data here (2A).

}

func RequestVote(peer *labrpc.ClientEnd, req *RequestVoteReq) (*RequestVoteRsp, error) {
	rsp := &RequestVoteRsp{}

	if ok := peer.Call("Raft.RequestVote", req, rsp); !ok {
		return nil, errors.New("call failed")
	}

	return rsp, nil
}
