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

// Add your RPC definitions here.

type GetTaskReq struct {
}

type GetTaskRsp struct {
    Ret int
    File string
}

type SubmitTaskReq struct {
    File string
    TmpFile string
}

type SubmitTaskRsp struct {
}

type GetReduceTaskReq struct {
}

type GetReduceTaskRsp struct {
    Ret int
    Files []string
    ReduceN int
    ReduceTotal int
}

type SubmitReduceTaskReq struct {
    ReduceN int
}

type SubmitReduceTaskRsp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
