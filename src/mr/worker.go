package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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

func ProcessMapTask(reply *Reply, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open file %v", reply.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)
	intermediateFiles := make([]*os.File, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d", reply.TaskID, i))
		if err != nil {
			log.Fatalf("cannot create temp file for reducer %d: %v", i, err)
		}
		defer tempFile.Close()
		intermediateFiles[i] = tempFile
	}
	//shuffle stage
	for _, kv := range intermediate {
		reduceTaskID := ihash(kv.Key) % reply.NReduce
		encoder := json.NewEncoder(intermediateFiles[reduceTaskID])
		err := encoder.Encode(kv)
		if err != nil {
			log.Fatalf("cannot write to file %v", intermediateFiles[reduceTaskID])
		}

	}

	//Atomic renaming
	for i := 0; i < reply.NReduce; i++ {
		finalFname := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		if err := os.Rename(intermediateFiles[i].Name(), finalFname); err != nil {
			log.Fatalf("cannot rename temp file %v to final file %v: %v", intermediateFiles[i].Name(), finalFname, err)
		}
	}

}

func ProcessReduceTask(reply *Reply, reducef func(string, []string) string) {
	reducerID := reply.TaskID - reply.NMap
	intermediateFiles := make([]*os.File, reply.NMap)
	for i := 0; i < reply.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reducerID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open file %v", fileName)
		}
		defer file.Close()
		intermediateFiles[i] = file
	}

	intermediate := []KeyValue{}
	for _, file := range intermediateFiles {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			fmt.Printf("The reduce input kv is %v \n", kv)
			intermediate = append(intermediate, kv)
		}
	}
	fmt.Printf("The reduced result for reducer task %d is %v \n", reply.TaskID, intermediate)

	// sort
	sort.Sort(ByKey(intermediate))

	// call reduce on each distinct key
	res := map[string]int{}
	for _, kv := range intermediate {
		res[kv.Key] += 1
	}
	outputFile, error := os.CreateTemp("", fmt.Sprintf("mr-out-%d", reducerID))
	if error != nil {
		log.Fatalf("cannot create file for reducer %v", reply.TaskID)
	}
	for key, value := range res {
		fmt.Fprintf(outputFile, "%v %v\n", key, strconv.Itoa(value))
	}
	finalFname := fmt.Sprintf("mr-out-%d", reducerID)
	if err := os.Rename(outputFile.Name(), finalFname); err != nil {
		log.Fatalf("cannot rename temp file %v to final file %v: %v", outputFile.Name(), finalFname, err)
	}

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		fmt.Fprintf(os.Stdout, "start sending request to the coordinator.. \n")
		reply := CallCoordinator()
		fmt.Fprintf(os.Stdout, "Finished rpc to the coordinator.. \n")
		if reply.TaskType == "map" {
			ProcessMapTask(reply, mapf)
			notifyCoordinator(reply.TaskID, reply.TaskType)
		}
		if reply.TaskType == "inprogress" {
			time.Sleep(1 * time.Second)
		}
		if reply.TaskType == "reduce" {
			ProcessReduceTask(reply, reducef)
			notifyCoordinator(reply.TaskID, reply.TaskType)
		}
		if reply.TaskType == "Done" {
			log.Printf("The worker %d has successfully completed its jobs! hoorray!", reply.TaskID)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argumen:=t and reply types are defined in rpc.go.
func CallCoordinator() *Reply {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).

	args.WorkerID = strconv.Itoa(os.Getpid())

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %s\n %d\n", reply.TaskType, reply.TaskID)
		return &reply

	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func notifyCoordinator(taskID int, taskType string) {
	args := Args{TaskID: taskID, TaskType: taskType}
	reply := Reply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Task %d has been successfully completed!\n", reply.TaskID)

	} else {
		fmt.Printf("call failed!\n")

	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	fmt.Printf("Connecting to coordinator socket: %s\n", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	fmt.Printf("Successfully Connected to coordinator socket\n")
	fmt.Printf("Calling RPC method: %s\n", rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		fmt.Printf("RPC call to %s succeeded.\n", rpcname)
		return true
	}
	fmt.Printf("Error in RPC call to %s: %v\n", rpcname, err)
	fmt.Println(err)
	return false
}
