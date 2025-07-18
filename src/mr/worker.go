package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMapTask(task *GetTaskReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("Map task received...\n")
	fmt.Printf("filename: %v\n", task.InputFileName)
	fileName := task.InputFileName
	contents, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		panic(err)
	}

	kva := mapf(fileName, string(contents))

	filecontentsmap := make(map[string]string)
	for _, kv := range kva {
		outputFileName := fmt.Sprintf("mr-%d-%d", task.OperationNumber, ihash(kv.Key)%task.NReduce)
		output := filecontentsmap[outputFileName]
		filecontentsmap[outputFileName] = fmt.Sprintf("%s%s %s\n", output, kv.Key, kv.Value)
	}

	for outputFileName, output := range filecontentsmap {
		file, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE, 0644)

		if err != nil {
			log.Fatalf("cannot open file %v", outputFileName)
			panic(err)
		}

		_, err = fmt.Fprintf(file, "%s", output)
		if err != nil {
			log.Fatalf("cannot write to file %v", outputFileName)
			panic(err)
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close file %v", outputFileName)
			panic(err)
		}
	}

	fmt.Printf("Map task completed: %v\n", task.InputFileName)
}

func parseKeyValuePairsFromFile(filename string) []KeyValue {
	// check if the file exists
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		log.Printf("file %v does not exist\n", filename)
		return []KeyValue{}
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v", filename)
		panic(err)
	}

	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
		panic(err)
	}

	// parse the key value pairs from the file
	kva := []KeyValue{}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		keyValue := strings.Fields(line)
		kva = append(kva, KeyValue{keyValue[0], keyValue[1]})
	}

	return kva
}

func handleReduceTask(task *GetTaskReply, reducef func(string, []string) string) {
	fmt.Printf("Reduce task %v received...\n", task.OperationNumber)

	intermediate := []KeyValue{}

	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.OperationNumber)
		kva := parseKeyValuePairsFromFile(filename)
		fmt.Printf("reduce task %v: got intermediate keys from %v\n", task.OperationNumber, filename)
		intermediate = append(intermediate, kva...)
	}

	fmt.Printf("reduce task %v: got intermediate keys\n", task.OperationNumber)

	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		outputFileName := fmt.Sprintf("mr-out-%d", task.OperationNumber)
		file, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("cannot open file %v", outputFileName)
			panic(err)
		}

		_, err = fmt.Fprintf(file, "%s %s\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write to file %v", outputFileName)
			file.Close()
			panic(err)
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close file %v", outputFileName)
			panic(err)
		}

		i = j
	}

	fmt.Printf("Reduce task %v completed\n", task.OperationNumber)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task, taskExists := GetTask()
		if !taskExists {
			break
		}
		if task.WaitForTask {
			// sleep  before trying to get a new task
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if task.Operation == "map" {
			handleMapTask(task, mapf)
		} else if task.Operation == "reduce" {
			handleReduceTask(task, reducef)
		} else {
			log.Fatalf("unknown operation: %v", task.Operation)
			panic(fmt.Errorf("unknown operation: %v", task.Operation))
		}

		MarkTaskCompleted(task.Operation, task.OperationNumber)
	}
}

func MarkTaskCompleted(operation string, operationNumber int) {
	args := MarkTaskCompletedArgs{
		Operation:       operation,
		OperationNumber: operationNumber,
	}
	reply := MarkTaskCompletedReply{}
	ok := call("Coordinator.MarkTaskCompleted", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func GetTask() (*GetTaskReply, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply, true
	} else {
		fmt.Printf("call failed!\n")
		return nil, false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
