package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distrubuted_system/labrpc"
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

	curTerm    int // current term number
	logEntries []*LogEntry
	votedIdx   int // who I voted for

	committedLogIdx   int       // highest log index known to be committed
	lastAppliedLogIdx int       // highest log index known to be applied to application
	nextSendLogIdx    []int     // index of log to be sent for each peer
	matchLogIdx       []int     // highest log index known to be replicated for each peer
	lastActiveTime    time.Time // last time a heartbeat is sent for leader or rpc received for follower

	state int

	peers []*labrpc.ClientEnd // RPC endpoints of all peers
	me    int                 // this peer's index into peers[]

	persister *Persister // object to hold this peer's persisted state
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
		me:        me,
		peers:     peers,
		persister: persister,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
	rf.changeState(StateKilled)
}

func (rf *Raft) changeState(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == StateLeader
}

func (rf *Raft) run(c chan ApplyMsg) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rf.applyMessageRoutine(ctx, c)

	rf.changeState(StateCandidate)
	go rf.runCandidate(ctx)

	tick := time.NewTicker(tickDuration)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			if state, _ := rf.GetState(); state == StateKilled {
				return
			}
		}
	}
}

func (rf *Raft) applyMessageRoutine(ctx context.Context, c chan ApplyMsg) {
	tick := time.NewTicker(tickDuration)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			// It's ok read stale committed index, hence no locking.
			if rf.committedLogIdx > rf.lastAppliedLogIdx {
				rf.lastAppliedLogIdx++

				c <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[rf.lastAppliedLogIdx],
					CommandIndex: rf.lastAppliedLogIdx,
				}
			}
		}
	}
}

func (rf *Raft) runLeader(ctx context.Context) {
	go rf.heartbeat(ctx)
}

func (rf *Raft) heartbeat(ctx context.Context) {
	tick := time.NewTicker(heartbeatInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			if time.Now().Sub(rf.lastActiveTime) < idleTimeout {
				continue
			}

			for _, peer := range rf.peers {
				go func(p *labrpc.ClientEnd) {
					_, _ = AppendEntries(
						ctx, p, &AppendEntriesReq{},
					)
				}(peer)
			}
		}
	}
}

func (rf *Raft) runCandidate(ctx context.Context) {
	var (
		ok  bool
		err error
	)

	for {
		waitRandomTime(time.Second)
		ok, err = rf.doElection(ctx)
		if err == nil {
			break
		}
	}

	if ok {
		rf.changeState(StateLeader)
		go rf.runLeader(ctx)
	} else {
		rf.changeState(StateFollower)
		go rf.runFollower(ctx)
	}
}

func (rf *Raft) doElection(ctx context.Context) (bool, error) {
	rf.curTerm++
	rf.votedIdx = rf.me

	ctx, cancel := context.WithTimeout(ctx, electionTimeout)
	defer cancel()

	req := &RequestVoteReq{}
	rspC := make(chan *RequestVoteRsp, len(rf.peers))

	for _, peer := range rf.peers {
		go func(p *labrpc.ClientEnd) {
			rsp, _ := RequestVote(ctx, p, req)
			rspC <- rsp

		}(peer)
	}

	rspCnt := 0
	gotVote := 0

	for {
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("election timeout")

		case rsp := <-rspC:
			rspCnt++
			if rsp != nil && rsp.Grant {
				gotVote++
			}

			if rspCnt == len(rf.peers) {
				return gotVote == majority(len(rf.peers)), nil
			}
		}
	}
}

func (rf *Raft) runFollower(ctx context.Context) {
	tick := time.NewTicker(tickDuration)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			if time.Now().Sub(rf.lastActiveTime) > heartbeatTimeout {
				rf.changeState(StateCandidate)
				go rf.runCandidate(ctx)
				return
			}
		}
	}
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rsp *AppendEntriesRsp) {
	rf.lastActiveTime = time.Now()
}

func (rf *Raft) RequestVote(req *RequestVoteReq, rsp *RequestVoteRsp) {
	// TODO

	// TODO set after granting vote
	rf.lastActiveTime = time.Now()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}
