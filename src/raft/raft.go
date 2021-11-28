package raft

import (
	"context"
	"sync"
	"time"

	"distrubuted_system/labrpc"
	"distrubuted_system/raft/client"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

type Raft struct {
	mu sync.Mutex // lock to protect shared access to this peer's state

	persistentState
	volatileState
	leaderVolatileState

	state  raftState
	stateC chan raftState

	peers []*labrpc.ClientEnd // RPC endpoints of all peers
	me    int                 // this peer's index into peers[]

	persister *Persister // object to hold this peer's persisted state
}

type persistentState struct {
	curTerm    int // current term number
	logEntries []*LogEntry
	votedIdx   int // who I voted for, -1 if none
}

type volatileState struct {
	committedLogIdx   int // highest log index known to be committed
	lastAppliedLogIdx int // highest log index known to be applied to application
}

type leaderVolatileState struct {
	nextSendLogIdx []int // index of log to be sent for each peer
	matchLogIdx    []int // highest log index known to be replicated for each peer
	lastActiveTime time.Time
}

type LogEntry struct {
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		state:     follower,
		stateC:    make(chan raftState, 1),
		me:        me,
		peers:     peers,
		persister: persister,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.stateC <- rf.state // push in the initial state
	go rf.run(applyCh)

	return rf
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	rf.changeState(killed)
}

func (rf *Raft) changeState(state raftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
	rf.stateC <- state
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == leader
}

func (rf *Raft) getRaftState() raftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) run(c chan ApplyMsg) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rf.applyMessageRoutine(ctx, c)

	for {
		select {
		case <-ctx.Done():
			return

		case state := <-rf.stateC:
			switch state {
			case killed:
				return

			case leader:
				go rf.runLeader(ctx)

			case follower:
				go rf.runFollower(ctx)

			case candidate:
				go rf.runCandidate(ctx)
			}
		}
	}
}

func (rf *Raft) applyMessageRoutine(ctx context.Context, c chan ApplyMsg) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.Tick(time.Second):
			rf.applyMessage(c)
		}
	}
}

func (rf *Raft) applyMessage(c chan ApplyMsg) {
	committedIdx, appliedIdx := rf.committedLogIdx, rf.lastAppliedLogIdx
	for committedIdx > appliedIdx {
		c <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[appliedIdx+1],
			CommandIndex: appliedIdx + 1,
		}
		rf.lastAppliedLogIdx++
	}
}

func (rf *Raft) runLeader(ctx context.Context) {
	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go rf.heartbeat(hbCtx)

	// TODO
}

func (rf *Raft) heartbeat(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.Tick(heartbeatInterval):
			if time.Now().Sub(rf.lastActiveTime) < idleTimeout {
				continue
			}

			// TODO send heartbeat

		}
	}
}

func (rf *Raft) runCandidate(ctx context.Context) {
	if ok := rf.doElection(); !ok {
		rf.changeState(follower)
	} else {
		rf.changeState(leader)
	}
}

func (rf *Raft) doElection() bool {
	rf.curTerm++
	rf.votedIdx = rf.me
	waitRandomTime(time.Second)

	// TODO implement election

	return true
}

func (rf *Raft) runFollower(ctx context.Context) {
}

func (rf *Raft) AppendEntries(req *client.AppendEntriesReq, rsp *client.AppendEntriesRsp) {

}

func (rf *Raft) RequestVote(req *client.RequestVoteReq, rsp *client.RequestVoteRsp) {

}
