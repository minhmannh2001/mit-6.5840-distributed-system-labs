package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetMapTaskArgs struct {
	WorkerID string
}

type GetMapTaskReply struct {
	KeyOfMapTask   string
	ValueOfMapTask Task
}

type ReturnMapTaskResultArgs struct {
	WorkerID       string
	KeyOfMapTask   string
	Status         int
	ReduceTaskKeys []string
}

type ReturnMapTaskResultReply struct{}

type GetReduceTaskArgs struct {
	WorkerID string
}

type GetReduceTaskReply struct {
	KeyOfReduceTask   string
	ValueOfReduceTask Task
}

type ReturnReduceTaskResultArgs struct {
	WorkerID     string
	KeyOfMapTask string
	Status       int
}

type ReturnReduceTaskResultReply struct{}

type IsAllMapTasksCompletedArgs struct{}

type IsAllMapTasksCompletedReply struct {
	Completed bool
}

type IsAllReduceTasksCompletedArgs struct{}

type IsAllReduceTasksCompletedReply struct {
	Completed bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
