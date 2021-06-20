package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	pendingMapFiles    []string
	pendingReduceTasks []int
	nMap               int
	nReduce            int
	runningMapTask     map[string]struct{}
	runningReduceTask  map[int]struct{}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	reduceTaskNo := make([]int, 0, nReduce)
	for i, _ := range reduceTaskNo {
		reduceTaskNo[i] = i
	}

	m := Master{
		pendingMapFiles:    files,
		pendingReduceTasks: reduceTaskNo,
		nMap:               len(files),
		nReduce:            nReduce,
		runningMapTask:     make(map[string]struct{}),
		runningReduceTask:  make(map[int]struct{}),
	}

	m.server()
	return &m
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()

	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return len(m.runningReduceTask) == 0 && len(m.runningMapTask) == 0 &&
		len(m.pendingReduceTasks) != 0 && len(m.pendingMapFiles) != 0
}

func (m *Master) AskTask(req *AskTaskReq, rsp *AskTaskRsp) error {
	if len(m.pendingMapFiles) != 0 {
		rsp = m.dispatchMap()
	}

	if len(m.pendingReduceTasks) != 0 {
		rsp = m.dispatchReduce()
	}

	rsp = &AskTaskRsp{
		Type: "DONE",
	}
	return nil
}

func (m *Master) dispatchMap() *AskTaskRsp {
	if len(m.pendingMapFiles) == 0 {
		return &AskTaskRsp{
			Type: "",
		}
	}

	filename := m.pendingMapFiles[0]
	m.pendingMapFiles = m.pendingMapFiles[1:]

	rsp := &AskTaskRsp{
		Type:      "MAP",
		Filename:  filename,
		MapSeq:    len(m.runningMapTask),
		ReduceSeq: m.nReduce,
	}

	m.runningMapTask[filename] = struct{}{}
	return rsp
}

func (m *Master) dispatchReduce() *AskTaskRsp {
	if len(m.pendingReduceTasks) == 0 {
		return &AskTaskRsp{
			Type: "DONE",
		}
	}

	reduceSeq := m.pendingReduceTasks[0]
	m.pendingReduceTasks = m.pendingReduceTasks[1:]

	rsp := &AskTaskRsp{
		Type:      "REDUCE",
		MapSeq:    m.nMap,
		ReduceSeq: reduceSeq,
	}

	m.runningReduceTask[reduceSeq] = struct{}{}
	return rsp
}

func (m *Master) MapDone(req *MapDoneReq, rsp *MapDoneRsp) error {
	delete(m.runningMapTask, req.SrcFilename)
	return nil
}

func (m *Master) ReduceDone(req *ReduceDoneReq, rsp *ReduceDoneRsp) error {
	delete(m.runningReduceTask, req.TaskSeq)
	return nil
}
