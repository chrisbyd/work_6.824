package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
		reduceTaskID := ihash(kv.Key) & reply.NReduce
		encoder := json.NewEncoder(intermediateFiles[reduceTaskID])
		err := encoder.Encode(kv)
		if err != nil {
			log.Fatalf("cannot write to file %c", intermediateFiles[reduceTaskID])
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

func ProcessReduceTask() {}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallCoordinator()
		if reply.TaskType == "map" {
			ProcessMapTask(reply, mapf)
		}
		if reply.TaskType == "inprogress" {
			time.Sleep(1 * time.Second)
		}
		if reply.TaskType == "reduce" {

		}
		if reply.TaskType == "Done" {
			log.Printf("The worker %d has successfully completed its jobs! hoorray!")
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
	args.WorkerID = "99"

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.requestTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %s\n %d\n", reply.TaskType, reply.TaskID)
		return &reply

	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func notifyCoordinator(taskID int) {
	args := Args{taskID: taskID}
	reply := Reply{}

	ok := call("Coordinator.completeTask", &args, &reply)
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
