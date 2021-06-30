package mr

import (
	"fmt"
	"github.com/pkg/errors"
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

			switch rsp.Type {
			case "MAP":
				if err := handleMap(rsp, mapf); err != nil {
					fmt.Println(err)
					return
				}
			case "REDUCE":
				if err := handleReduce(rsp, reducef); err != nil {
					fmt.Println(err)
					return
				}
			case "DONE":
				return
			}
		}
	}
}

func handleMap(rsp *AskTaskRsp, mapf func(string, string) []KeyValue) error {
	fmt.Println("worker handle map:", rsp.MapSeq)

	filename := rsp.Filename

	content, err := getFileContent(filename)
	if err != nil {
		return errors.Wrap(err, "get file content failed")
	}

	kvs := mapf(filename, content)
	kvGroups := splitKeyValuesToReduceGroup(kvs, rsp.ReduceSeq)

	for i, kvGroup := range kvGroups {
		kvString, err := keyValuesToString(kvGroup)
		if err != nil {
			return errors.Wrap(err, "kvs to string failed")
		}

		outputFilename := fmt.Sprintf("mr-%d-%d", rsp.MapSeq, i)
		if err := writeStringToFile(kvString, outputFilename); err != nil {
			return errors.Wrap(err, "write string to file failed")
		}
	}

	mapDone(&MapDoneReq{Seq: rsp.MapSeq})
	return nil
}

func getFileContent(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", errors.Wrap(err, "open file failed")
	}

	content, err := ioutil.ReadAll(f)
	if err != nil {
		return "", errors.Wrap(err, "read file failed")
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

func keyValuesToString(kvs []KeyValue) (string, error) {
	b := strings.Builder{}
	for _, kv := range kvs {
		if _, err := b.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value)); err != nil {
			return "", errors.Wrap(err, "write file failed")
		}
	}
	return b.String(), nil
}

func writeStringToFile(content string, filename string) error {
	f, err := ioutil.TempFile(".", filename)
	if err != nil {
		return errors.Wrap(err, "open temp file failed")
	}

	if err := os.Chmod(f.Name(), 0644); err != nil {
		return errors.Wrap(err, "chmod failed")
	}

	_, err = f.WriteString(content)
	if err != nil {
		return errors.Wrap(err, "write file failed")
	}

	if _, err = os.Stat(filename); os.IsNotExist(err) {
		fmt.Println("commit file:", filename)
		return errors.Wrap(os.Rename(f.Name(), filename), "rename file failed")
	}
	fmt.Println("delete file:", f.Name())
	return errors.Wrap(os.Remove(f.Name()), "remove file failed")
}

func handleReduce(rsp *AskTaskRsp, reducef func(string, []string) string) error {
	fmt.Println("worker handle reduce:", rsp.ReduceSeq)

	kvs, err := aggregateReduceKeyValues(rsp.ReduceSeq, rsp.MapSeq)
	if err != nil {
		return errors.Wrap(err, "aggregate kvs failed")
	}

	b := strings.Builder{}
	for k, vs := range kvs {
		res := reducef(k, vs)
		if _, err := b.WriteString(fmt.Sprintf("%v %v\n", k, res)); err != nil {
			return errors.Wrap(err, "write file failed")
		}
	}

	outputFilename := fmt.Sprintf("mr-out-%d", rsp.ReduceSeq)
	if err := writeStringToFile(b.String(), outputFilename); err != nil {
		return errors.Wrap(err, "write string to file failed")
	}

	reduceDone(&ReduceDoneReq{Seq: rsp.ReduceSeq})
	return nil
}

func aggregateReduceKeyValues(reduceSeq int, mapCount int) (map[string][]string, error) {
	res := make(map[string][]string)

	kvs, err := readAllKeyValues(reduceSeq, mapCount)
	if err != nil {
		return nil, errors.Wrap(err, "read all kvs failed")
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
			return nil, errors.Wrap(err, "get file content failed")
		}

		kvs := parseStringToKeyValues(content)
		res = append(res, kvs...)

		if err := os.Remove(filename); err != nil {
			return nil, errors.Wrap(err, "remove file failed")
		}
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
