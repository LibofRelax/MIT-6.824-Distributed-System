package mr

type AskTaskReq struct {
}

type AskTaskRsp struct {
	Type      string
	Filename  string
	MapSeq    int // map task seq number if it this is a map task, else total map task count
	ReduceSeq int
}

func askTask(req *AskTaskReq) (*AskTaskRsp, bool) {
	rsp := &AskTaskRsp{}
	if ok := call("Master.AskTask", req, rsp); !ok {
		return nil, ok
	}
	return rsp, true
}

type MapDoneReq struct {
	SrcFilename string
}

type MapDoneRsp struct {
}

func mapDone(req *MapDoneReq) (*MapDoneRsp, bool) {
	rsp := &MapDoneRsp{}
	if ok := call("Master.MapDone", req, rsp); !ok {
		return nil, ok
	}
	return rsp, true
}

type ReduceDoneReq struct {
	TaskSeq int
}

type ReduceDoneRsp struct {
}

func reduceDone(req *ReduceDoneReq) (*ReduceDoneRsp, bool) {
	rsp := &ReduceDoneRsp{}
	if ok := call("Master.ReduceDone", req, rsp); !ok {
		return nil, ok
	}
	return rsp, true
}
