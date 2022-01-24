package raft

import (
	"fmt"
	"log"
	"sync"
	"time"

	"distrubuted_system/labrpc"
	"distrubuted_system/raft/client"
	"distrubuted_system/raft/schema"
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
	mu        sync.Mutex // lock to protect shared access to this peer's state
	persister *Persister // object to hold this peer's persisted state

	logEntries []*schema.LogEntry

	curTerm int // current term number
	role    RaftRole

	committedLogIdx   int       // highest log index known to be committed
	lastAppliedLogIdx int       // highest log index known to be applied to application
	nextSendLogIdx    []int     // index of log to be sent for otherPeers
	matchLogIdx       []int     // highest log index known to be replicated for otherPeers
	lastActiveTime    time.Time // last time a heartbeat is sent for leader or rpc received for follower

	peers      []*labrpc.ClientEnd // RPC endpoints of all peers
	otherPeers []*labrpc.ClientEnd
	me         int // this peer's index into peers[]
	votedIdx   int // who I voted for

	convertToFollowerSignal chan struct{}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{
		me:        me,
		peers:     peers,
		persister: persister,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.run(applyCh)
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
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, false
	}

	// TODO append entries

	return len(rf.logEntries) - 1, term, true
}

func (rf *Raft) Kill() {
	rf.convertToRole(RaftRoleDead)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.role == RaftRoleLeader
}

func (rf *Raft) convertToRole(role RaftRole) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) signalConvertToFollower() {
	rf.convertToFollowerSignal <- struct{}{}
}

func (rf *Raft) Logf(format string, v ...interface{}) {
	format = fmt.Sprintf("Me: %d, %s. Role: %s, Term: %d", rf.me, format, rf.role, rf.curTerm)
	log.Printf(format, v...)
}

func (rf *Raft) run(c chan<- ApplyMsg) {
	rf.initInternalStates()
	rf.backgroundApplyMessage(c)
	rf.runFollower()
	return
}

func (rf *Raft) initInternalStates() {
	rf.votedIdx = -1
	rf.lastActiveTime = time.Now().Add(-randomDuration(electionMaxWaitTime))
	rf.matchLogIdx = make([]int, len(rf.peers)-1)
	rf.nextSendLogIdx = make([]int, len(rf.peers)-1)
	rf.otherPeers = make([]*labrpc.ClientEnd, 0, len(rf.peers)-1)
	rf.convertToFollowerSignal = make(chan struct{})

	for i, p := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.otherPeers = append(rf.otherPeers, p)
	}
}

func (rf *Raft) backgroundApplyMessage(c chan<- ApplyMsg) {
	go func() {
		tick := time.NewTicker(tickDuration)
		defer tick.Stop()

		for range tick.C {
			if rf.role == RaftRoleDead {
				break
			}

			// It's ok read stale committed index, hence no locking.
			for rf.committedLogIdx > rf.lastAppliedLogIdx {
				rf.lastAppliedLogIdx++
				logEntry := rf.logEntries[rf.lastAppliedLogIdx]

				c <- ApplyMsg{
					CommandValid: logEntry.Valid,
					Command:      logEntry.Command,
					CommandIndex: rf.lastAppliedLogIdx,
				}
			}
		}
	}()
}

func (rf *Raft) runFollower() {
	rf.convertToRole(RaftRoleFollower)
	randomTimeoutOffset := randomDuration(electionMaxWaitTime)

	go func() {
		tick := time.NewTicker(tickDuration)
		defer tick.Stop()

		for range tick.C {
			if time.Now().Sub(rf.lastActiveTime) > electionTimeout+randomTimeoutOffset {
				rf.runCandidate()
				return
			}
		}
	}()
}

func (rf *Raft) runCandidate() {
	rf.convertToRole(RaftRoleCandidate)

	go func() {
		for {
			select {
			case <-rf.convertToFollowerSignal:
				rf.runFollower()
				return

			default:
				for !rf.doElection() {
					time.Sleep(randomDuration(electionMaxWaitTime))
				}

				rf.runLeader()
				return
			}
		}
	}()
}

func (rf *Raft) doElection() (done bool) {
	rf.curTerm++
	rf.votedIdx = rf.me

	rf.Logf("requesting votes")

	lastLogTerm := 0
	if l := len(rf.logEntries); l > 0 {
		lastLogTerm = rf.logEntries[l-1].Term
	}

	rspC := make(chan *schema.RequestVoteRsp, len(rf.otherPeers))

	for _, peer := range rf.otherPeers {
		peer := peer
		go func() {
			rspC <- client.RequestVote(peer, &schema.RequestVoteReq{
				Term:         rf.curTerm,
				CandidateIdx: rf.me,
				LastLogIdx:   len(rf.logEntries),
				LastLogTerm:  lastLogTerm,
			})
		}()
	}

	gotVote := 1 // always one vote from self

	for i := 0; i < len(rf.otherPeers); i++ {
		rsp := <-rspC
		if rsp == nil {
			continue
		}

		if rsp.Grant {
			gotVote++
		}
	}

	rf.Logf("got %d votes", gotVote)

	return gotVote >= majorityOf(len(rf.peers))
}

func (rf *Raft) runLeader() {
	rf.convertToRole(RaftRoleLeader)

	rf.sendHeartbeat()

	go func() {
		heartbeatTick := time.NewTicker(heartbeatInterval)
		defer heartbeatTick.Stop()

		for {
			select {
			case <-rf.convertToFollowerSignal:
				rf.runFollower()
				return

			case <-heartbeatTick.C:
				if rf.role != RaftRoleLeader {
					break
				}

				if time.Now().Sub(rf.lastActiveTime) > idleTimeout {
					rf.sendHeartbeat()
				}

			default:

			}
		}
	}()
}

func (rf *Raft) sendHeartbeat() {
	for i, peer := range rf.otherPeers {
		i, peer := i, peer
		go func() {
			rsp := client.AppendEntries(peer, &schema.AppendEntriesReq{
				Term:      rf.curTerm,
				LeaderIdx: rf.me,
			})

			if rsp == nil {
				rf.Logf("failed to send heartbeat to %d", i)
			}
		}()
	}
}

func (rf *Raft) AppendEntries(req *schema.AppendEntriesReq, rsp *schema.AppendEntriesRsp) {
	rf.Logf("receive AppendEntriesReq: %+v", req)

	switch rf.role {
	case RaftRoleCandidate:
		if req.Term > rf.curTerm {
			rf.Logf("new leader: %d detected, convert to follower", req.LeaderIdx)

			rf.curTerm = req.Term
			rf.lastActiveTime = time.Now()
			rf.signalConvertToFollower()
		}

	case RaftRoleFollower:
		rf.curTerm = req.Term
		rf.lastActiveTime = time.Now()

	case RaftRoleLeader:

	}
}

func (rf *Raft) RequestVote(req *schema.RequestVoteReq, rsp *schema.RequestVoteRsp) {
	rf.Logf("receive RequestVoteReq: %+v", req)
	defer func() {
		rf.Logf("return RequestVoteRsp: %+v", rsp)
	}()

	rsp.Term = rf.curTerm

	if rf.role != RaftRoleFollower {
		rsp.Grant = false
		return
	}

	higherTermThanMe := req.Term > rf.curTerm
	higherLogIndexThanMe := req.Term == rf.curTerm && req.LastLogIdx > len(rf.logEntries)

	rsp.Grant = higherTermThanMe || higherLogIndexThanMe
	if rsp.Grant {
		rf.votedIdx = req.CandidateIdx
	}
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
