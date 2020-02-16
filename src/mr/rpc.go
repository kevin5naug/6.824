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

// WorkerRPC provides necessary information to register this worker for RPC call
type WorkerRPC struct {
	ID string
	Mapf func(string, string) []KeyValue
	Reducef func(string, []string) string
	DoneCh chan bool
	OtherCh chan bool
}

// WorkerRegArgs give master the worker id used for new worker registration
type WorkerRegArgs struct {
	WorkerID string
}

type TaskArgs struct {
	TaskID int
	TaskType string //"map" or "reduce"
	FilePath string //only has meaning if TaskType = "map"
	R int //only has meaning if TaskType = "map"
	M int //only has meaning if TaskType = "reduce"
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func workerSock(workerID string) string {
	s := "/var/tmp/824-mr-"
	s += workerID
	return s
}
