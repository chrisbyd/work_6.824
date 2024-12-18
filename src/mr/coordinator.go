package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	done              bool
	nMap              int
	nReduce           int
	mapTasks          []string //未分配的tasks数量
	reduceTasks       []int
	inProgress        map[int]string // taskid to worker id
	completed         map[int]bool
	taskDetails       map[int]string    // task id to task information e.g. task filename
	taskDeadline      map[int]time.Time //task id to task timeout duration
	timeout           time.Duration     //the 」
	completedMapTasks map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) requestTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//map task
	if len(c.mapTasks) > 0 {
		task := c.mapTasks[0]
		c.mapTasks = c.mapTasks[1:]
		reply.TaskType = "map"
		reply.FileName = task
		reply.TaskID = c.nMap - len(c.mapTasks) - 1
		reply.NReduce = c.nReduce
		//coordinator states
		c.taskDetails[reply.TaskID] = task
		c.inProgress[reply.TaskID] = args.WorkerID
		return nil
	}

	// reduce task
	if len(c.reduceTasks) > 0 {
		task := c.reduceTasks[0]
		c.reduceTasks = c.reduceTasks[1:]
		reply.TaskType = "reduce"
		reply.TaskID = task
		reply.NReduce = c.nReduce
		c.inProgress[reply.TaskID] = args.WorkerID
		return nil
	}
	// done task

	if len(c.inProgress) > 0 {
		reply.TaskType = "inprogress"
		return nil
	}
	reply.TaskType = "done"
	return nil
}

func (c *Coordinator) completeTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == "map" {
		if 
	}
	if args.TaskType == "reduce" {
	}

	//

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.Done()

	return ret
}

func constructMapTasks(files []string, c *Coordinator) {
	c.mapTasks = append([]string{}, files...)
	c.reduceTasks = make([]int, 0)
	c.inProgress = make(map[int]string)

}

func constructReduceTasks(c *Coordinator) {
	c.reduceTasks = make([]int, 0)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = i
	}
}

func (c *Coordinator) checkTimeout() {
	for {
		c.mu.Lock()
		defer c.mu.Unlock()
		for taskID, deadline := range c.taskDeadline {
			if time.Now().After(deadline) && !c.completed[taskID] {
				fmt.Printf("Task id %d has timed out,.. reassigining ..", taskID)

				//任务超时，加入队列
				if taskID < c.nMap {
					c.mapTasks = append(c.mapTasks, c.taskDetails[taskID])
				}
				if taskID >= c.nMap {
					c.reduceTasks = append(c.reduceTasks, taskID-c.nMap)
				}
				delete(c.inProgress, taskID)
				delete(c.taskDetails, taskID)
			}
		}
	}

}

func (c *Coordinator) checkIfMapComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.inProgress) == 0 {
		constructReduceTasks(c)
		return true
	}
	return false

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	if nReduce < 1 {
		fmt.Fprintf(os.Stderr, "The number of reducers should be larger than 1")
		os.Exit(1)
	}
	constructMapTasks(files, &c)
	c.timeout = time.Second * 10
	

	// Your code here.

	c.server()
	return &c
}
