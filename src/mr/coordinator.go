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
func (c *Coordinator) RequestTask(args *Args, reply *Reply) error {
	fmt.Printf("Received work request from work id %s \n ", args.WorkerID)
	c.mu.Lock()
	defer c.mu.Unlock()
	//map task\
	fmt.Printf("mutex locak has been applied \n")

	if len(c.mapTasks) > 0 {
		task := c.mapTasks[0]
		c.mapTasks = c.mapTasks[1:]
		reply.TaskType = "map"
		reply.FileName = task
		reply.TaskID = c.nMap - len(c.mapTasks) - 1
		reply.NReduce = c.nReduce
		//coordinator states

		fmt.Printf("Assigned map task: %s, Task ID: %d\n", reply.FileName, reply.TaskID)

		c.taskDetails[reply.TaskID] = task
		c.inProgress[reply.TaskID] = args.WorkerID
		c.taskDeadline[reply.TaskID] = time.Now().Add(c.timeout)
		return nil
	}

	// reduce task
	if len(c.reduceTasks) > 0 {
		task := c.reduceTasks[0]
		c.reduceTasks = c.reduceTasks[1:]
		reply.TaskType = "reduce"
		reply.TaskID = task + c.nMap
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		fmt.Printf("Assigned reduce task: Task ID: %d\n", reply.TaskID)

		c.inProgress[reply.TaskID] = args.WorkerID
		c.taskDeadline[reply.TaskID] = time.Now().Add(c.timeout)
		return nil
	}
	// done task
	fmt.Printf("The inProgress map is %v \n", c.inProgress)
	if len(c.inProgress) > 0 {
		reply.TaskType = "inprogress"
		return nil
	}
	reply.TaskType = "done"
	fmt.Println("All tasks are completed. Returning 'done'.")
	return nil
}

func (c *Coordinator) CompleteTask(args *Args, reply *Reply) error {
	c.mu.Lock()

	defer c.mu.Unlock()

	if c.completed[args.TaskID] {
		fmt.Printf("This task has already been completed by other workers\n")
		return nil
	}
	fmt.Printf("The task has not been completed by other workers. \n ")

	currentWorker, exists := c.inProgress[args.TaskID]
    if !exists {
        fmt.Printf("Task %d is not in progress or has already been reassigned.\n", args.TaskID)
        return nil
    }

    if currentWorker != args.WorkerID {
        fmt.Printf("Task %d completion reported by Worker %s, but current assigned Worker is %s. Ignoring.\n",
            args.TaskID, args.WorkerID, currentWorker)
        return nil
    }

	delete(c.inProgress, args.TaskID)
	fmt.Printf("Task %d has been deleted from inProgess map. \n ", args.TaskID)
	delete(c.taskDeadline, args.TaskID)
	fmt.Printf("Task %d has been deleted from taskDeadline map. \n ", args.TaskID)
	c.completed[args.TaskID] = true
	if args.TaskType == "map" {
		delete(c.taskDetails, args.TaskID)
		fmt.Printf("Task %d has been deleted from taskDetails map. \n ", args.TaskID)
	}
	if len(c.inProgress) == 0 && args.TaskType == "map" {
		fmt.Printf("Start constructing reduce tasks!")
		constructReduceTasks(c)
		c.completed = make(map[int]bool)
	}
	if args.TaskType == "reduce" && c.allReduceTasksCompleted() {
        fmt.Printf("All reduce tasks completed. Job is done.\n")
        c.done = true
    }

	fmt.Printf("Ok! Now/ Task %d has been successfully completed. \n ", args.TaskID)
	return nil

}

func (c *Coordinator) allReduceTasksCompleted() bool {
    for taskID := c.nMap; taskID < c.nMap+c.nReduce; taskID++ {
        if !c.completed[taskID] {
            return false
        }
    }
    return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	fmt.Printf("Coordinator socket path: %s\n", sockname)
	os.Remove(sockname)

	fmt.Println("Coordinator is starting the server...")

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
    defer c.mu.Unlock()
    return c.done

}

func constructMapTasks(files []string, c *Coordinator) {
	c.mapTasks = append([]string{}, files...)
	c.reduceTasks = make([]int, 0)
	c.inProgress = make(map[int]string)
	c.taskDetails = make(map[int]string)
	c.taskDeadline = make(map[int]time.Time)
	c.completed = make(map[int]bool)
	fmt.Fprintf(os.Stdout, "Finish map task construction\n")
}

func constructReduceTasks(c *Coordinator) {
	c.reduceTasks = make([]int, 0)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, i)
	}
}

func (c *Coordinator) checkTimeout() {
	fmt.Printf("So start checking the timeout of task\n")
	for {
		c.mu.Lock()

		for taskID, deadline := range c.taskDeadline {
			if time.Now().After(deadline) && !c.completed[taskID] {
				fmt.Printf("Task id %d has timed out,.. reassigining ..\n", taskID)
				fmt.Printf("The inprogress map is %v \n", c.inProgress)
				//任务超时，加入队列
				if taskID < c.nMap {
					c.mapTasks = append(c.mapTasks, c.taskDetails[taskID])
				}
				if taskID >= c.nMap {
					c.reduceTasks = append(c.reduceTasks, taskID-c.nMap)
				}
				delete(c.inProgress, taskID)
				fmt.Printf("The inprogress map after deletion is %v", c.inProgress)
				delete(c.taskDetails, taskID)
				delete(c.taskDeadline, taskID)
			}
		}
		c.mu.Unlock()
		time.Sleep(1 * time.Second)
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
	c.nMap = len(files)
	c.nReduce = nReduce
	constructMapTasks(files, &c)
	c.timeout = time.Second * 10

	// Your code here.
	go c.checkTimeout()
	

	c.server()
	return &c
}
