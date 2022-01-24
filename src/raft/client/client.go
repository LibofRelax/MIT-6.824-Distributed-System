package client

import (
	"distrubuted_system/labrpc"
	"distrubuted_system/raft/schema"
)

func AppendEntries(peer *labrpc.ClientEnd, req *schema.AppendEntriesReq) *schema.AppendEntriesRsp {
	rsp := &schema.AppendEntriesRsp{}
	if ok := peer.Call("Raft.AppendEntries", req, rsp); !ok {
		return nil
	}
	return rsp
}

func RequestVote(peer *labrpc.ClientEnd, req *schema.RequestVoteReq) *schema.RequestVoteRsp {
	rsp := &schema.RequestVoteRsp{}
	if ok := peer.Call("Raft.RequestVote", req, rsp); !ok {
		return nil
	}
	return rsp
}
