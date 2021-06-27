package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rsp, ok := askTask(&AskTaskReq{})
			if !ok {
				fmt.Println("worker RPC fail")
				return
			}
			fmt.Println("worker got task: ", rsp)

			switch rsp.Type {
			case "MAP":
				handleMap(rsp, mapf)
			case "REDUCE":
				handleReduce(rsp, reducef)
			case "DONE":
				return
			}
		}
	}
}

func handleMap(rsp *AskTaskRsp, mapf func(string, string) []KeyValue) {
	filename := rsp.Filename

	content, err := getFileContent(filename)
	if err != nil {
		panic(err)
	}

	kvs := mapf(filename, content)
	kvGroups := splitKeyValuesToReduceGroup(kvs, rsp.ReduceSeq)

	for i, kvGroup := range kvGroups {
		kvString := keyValuesToString(kvGroup)

		outputFilename := fmt.Sprintf("mr-%d-%d", rsp.MapSeq, i)
		if err := writeStringToFile(kvString, outputFilename); err != nil {
			return
		}
	}

	mapDone(&MapDoneReq{SrcFilename: filename})
}

func getFileContent(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}

	content, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func splitKeyValuesToReduceGroup(kvs []KeyValue, nReduce int) [][]KeyValue {
	res := make([][]KeyValue, nReduce, nReduce)
	for i, _ := range res {
		res[i] = make([]KeyValue, 0)
	}

	for _, kv := range kvs {
		reduceIndex := ihash(kv.Key) % nReduce
		res[reduceIndex] = append(res[reduceIndex], kv)
	}
	return res
}

func keyValuesToString(kvs []KeyValue) string {
	b := strings.Builder{}
	for _, kv := range kvs {
		b.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
	}
	return b.String()
}

func writeStringToFile(content string, filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	_, err = f.WriteString(content)
	return err
}

func handleReduce(rsp *AskTaskRsp, reducef func(string, []string) string) {
	kvs, err := aggregateReduceKeyValues(rsp.ReduceSeq, rsp.MapSeq)
	if err != nil {
		panic(err)
	}

	b := strings.Builder{}
	for k, vs := range kvs {
		res := reducef(k, vs)
		b.WriteString(fmt.Sprintf("%v %v\n", k, res))
	}

	outputFilename := fmt.Sprintf("mr-out-%d", rsp.ReduceSeq)
	writeStringToFile(b.String(), outputFilename)

	reduceDone(&ReduceDoneReq{TaskSeq: rsp.ReduceSeq})
}

func aggregateReduceKeyValues(reduceSeq int, mapCount int) (map[string][]string, error) {
	res := make(map[string][]string)

	kvs, err := readAllKeyValues(reduceSeq, mapCount)
	if err != nil {
		return nil, err
	}

	for _, kv := range kvs {
		if _, ok := res[kv.Key]; !ok {
			res[kv.Key] = make([]string, 0)
		}

		res[kv.Key] = append(res[kv.Key], kv.Value)
	}
	return res, nil
}

func readAllKeyValues(reduceSeq int, mapCount int) ([]KeyValue, error) {
	res := make([]KeyValue, 0)
	for mapSeq := 0; mapSeq < mapCount; mapSeq++ {
		filename := fmt.Sprintf("mr-%d-%d", mapSeq, reduceSeq)

		content, err := getFileContent(filename)
		if err != nil {
			return nil, err
		}

		kvs := parseStringToKeyValues(content)
		res = append(res, kvs...)
	}
	return res, nil
}

func parseStringToKeyValues(str string) []KeyValue {
	lines := strings.Split(str, "\n")
	res := make([]KeyValue, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		var key, value string
		_, _ = fmt.Sscanf(line, "%s %s", &key, &value)

		res = append(
			res, KeyValue{
				Key:   key,
				Value: value,
			},
		)
	}

	return res
}
