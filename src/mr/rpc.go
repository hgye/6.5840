package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Add your RPC definitions here.
// work get a task from coordinator rpc
type TaskReqArgs struct {
	Id int // using pid as worker ID
} // just nothing

type TaskReqReply struct {
	WorkerId     int // assign by coordinator, such as m1 for mapper worker1
	Phase        Phase
	Filename     string // filename for map stage
	NReduce      int
	Intermediate []string // filename list for map stage generated, such as mr-m1-r1
}

type TaskAckArgs struct {
	Id       int
	Phase    Phase
	Filename string // for map phase
	Succ     bool
}

type TaskAckReply struct {
}

type ResultArgs struct {
	Out []string
}
type ResultReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
