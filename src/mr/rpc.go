package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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
