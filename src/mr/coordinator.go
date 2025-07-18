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

type TaskStatus int

const (
	NotStarted TaskStatus = iota
	Assigned
	Completed
)

type Task struct {
	taskStatus TaskStatus
	assignedAt time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	Files       []string
	MapTasks    []Task
	ReduceTasks []Task
	done        chan struct{}
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	InputFileName   string
	Operation       string
	OperationNumber int
	NMap            int
	NReduce         int
	WaitForTask     bool
}

var ErrAllTasksCompleted = fmt.Errorf("all tasks completed")

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if i, task := c.findAvailableTask(c.MapTasks); task != nil {
		c.assignTask("map", i, task, reply)
		return nil
	}

	if !c.allTasksCompleted(c.MapTasks) {
		reply.WaitForTask = true
		fmt.Println("No map tasks available, worker should wait")
		return nil
	}

	fmt.Println("All map tasks completed, looking for reduce tasks")

	if i, task := c.findAvailableTask(c.ReduceTasks); task != nil {
		c.assignTask("reduce", i, task, reply)
		return nil
	}

	if c.allTasksCompleted(c.ReduceTasks) {
		return ErrAllTasksCompleted
	}

	reply.WaitForTask = true
	fmt.Println("No reduce tasks available, worker should wait")
	return nil
}

func (c *Coordinator) findAvailableTask(tasks []Task) (int, *Task) {
	for i, task := range tasks {
		if task.taskStatus == NotStarted {
			return i, &tasks[i]
		}
	}
	return 0, nil
}

func (c *Coordinator) assignTask(operation string, index int, task *Task, reply *GetTaskReply) {
	reply.Operation = operation
	reply.OperationNumber = index
	reply.NMap = len(c.MapTasks)
	reply.NReduce = len(c.ReduceTasks)
	reply.WaitForTask = false
	task.taskStatus = Assigned
	task.assignedAt = time.Now()

	if operation == "map" {
		reply.InputFileName = c.Files[index]
	}
}

func (c *Coordinator) allTasksCompleted(tasks []Task) bool {
	for _, task := range tasks {
		if task.taskStatus != Completed {
			return false
		}
	}
	return true
}

type MarkTaskCompletedArgs struct {
	Operation       string
	OperationNumber int
}

type MarkTaskCompletedReply struct{}

func (c *Coordinator) MarkTaskCompleted(args *MarkTaskCompletedArgs, reply *MarkTaskCompletedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Operation == "map" {
		c.MapTasks[args.OperationNumber].taskStatus = Completed
		return nil
	} else if args.Operation == "reduce" {
		c.ReduceTasks[args.OperationNumber].taskStatus = Completed
		return nil
	}
	return fmt.Errorf("invalid operation")
}

func (c *Coordinator) AllMapTasksCompleted() bool {
	for _, task := range c.MapTasks {
		if task.taskStatus != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllReduceTasksCompleted() bool {
	for _, task := range c.ReduceTasks {
		if task.taskStatus != Completed {
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
	if c.AllMapTasksCompleted() && c.AllReduceTasksCompleted() {
		close(c.done)
		return true
	}

	return false
}

func (c *Coordinator) startPeriodicChecks() {

	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				c.checkTimeoutsAndReassignTasks()
			case <-c.done:
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Coordinator) checkTimeoutsAndReassignTasks() {
	fmt.Printf("coordinator: checking for timed out tasks\n")
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, task := range c.MapTasks {
		if task.taskStatus == Assigned && time.Since(task.assignedAt) > 10*time.Second {
			fmt.Printf("coordinator: map task %v timed out, reassigning\n", i)
			c.MapTasks[i] = Task{}
		}
	}

	for i, task := range c.ReduceTasks {
		if task.taskStatus == Assigned && time.Since(task.assignedAt) > 10*time.Second {
			fmt.Printf("coordinator: reduce task %v timed out, reassigning\n", i)
			c.ReduceTasks[i] = Task{}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:       files,
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
		done:        make(chan struct{}),
	}
	fmt.Printf("Coordinator: MakeCoordinator\n")
	fmt.Printf("Coordinator: files %v\n", files)
	fmt.Printf("Coordinator: map tasks %v\n", c.MapTasks)
	fmt.Printf("Coordinator: reduce tasks %v\n", c.ReduceTasks)

	c.startPeriodicChecks()
	c.server()
	return &c
}
