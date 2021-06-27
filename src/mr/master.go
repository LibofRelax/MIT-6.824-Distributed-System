package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	mutex sync.Mutex

	pendingMapFiles    []string
	pendingReduceTasks []int
	curMapSeq          int
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
	reduceTaskNo := make([]int, nReduce, nReduce)
	for i, _ := range reduceTaskNo {
		reduceTaskNo[i] = i
	}

	m := Master{
		pendingMapFiles:    files,
		pendingReduceTasks: reduceTaskNo,
		curMapSeq:          0,
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
		len(m.pendingReduceTasks) == 0 && len(m.pendingMapFiles) == 0
}

func (m *Master) AskTask(req *AskTaskReq, rsp *AskTaskRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.pendingMapFiles) != 0 {
		fmt.Println("master dispatch MAP task")
		m.dispatchMap(rsp)
		return nil
	}
	if len(m.runningMapTask) != 0 {
		rsp.Type = ""
		return nil
	}

	if len(m.pendingReduceTasks) != 0 {
		fmt.Println("master dispatch REDUCE task")
		m.dispatchReduce(rsp)
		return nil
	}

	rsp.Type = "DONE"
	return nil
}

func (m *Master) dispatchMap(rsp *AskTaskRsp) {
	if len(m.pendingMapFiles) == 0 {
		return
	}

	filename := m.pendingMapFiles[0]
	m.pendingMapFiles = m.pendingMapFiles[1:]

	rsp.Type = "MAP"
	rsp.Filename = filename
	rsp.MapSeq = m.curMapSeq
	rsp.ReduceSeq = m.nReduce

	fmt.Println("dispatch: ", rsp)

	m.curMapSeq++
	m.runningMapTask[filename] = struct{}{}
}

func (m *Master) dispatchReduce(rsp *AskTaskRsp) {
	if len(m.pendingReduceTasks) == 0 {
		rsp.Type = "DONE"
		return
	}

	reduceSeq := m.pendingReduceTasks[0]
	m.pendingReduceTasks = m.pendingReduceTasks[1:]

	rsp.Type = "REDUCE"
	rsp.MapSeq = m.curMapSeq
	rsp.ReduceSeq = reduceSeq

	fmt.Println("dispatch: ", rsp)

	m.runningReduceTask[reduceSeq] = struct{}{}
}

func (m *Master) MapDone(req *MapDoneReq, rsp *MapDoneRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Printf("map %s done\n", req.SrcFilename)
	delete(m.runningMapTask, req.SrcFilename)
	return nil
}

func (m *Master) ReduceDone(req *ReduceDoneReq, rsp *ReduceDoneRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Printf("reduce %d done\n", req.TaskSeq)
	delete(m.runningReduceTask, req.TaskSeq)
	return nil
}
