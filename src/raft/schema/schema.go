package schema

type LogEntry struct {
	Term    int
	Valid   bool
	Command interface{}
}

type AppendEntriesReq struct {
	Term               int
	LeaderIdx          int
	LeaderCommittedIdx int
	PrevLogIdx         int
	Entries            []*LogEntry
}

type AppendEntriesRsp struct {
	Term    int
	Success bool
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
