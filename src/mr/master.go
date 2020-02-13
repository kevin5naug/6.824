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
	"strconv"
)

//
type Master struct {
	// Your definitions here.
	sync.Mutex    //embedded field
	Files         []string
	R             int
	WorkerList    []string
	NewWorkerCond *sync.Cond
	L             net.Listener
	DoneCh        chan bool
}

//
// Your code here -- RPC handlers for the worker to call.
func (m *Master) RegisterNewWorker(args *WorkerRegArgs, _ *struct{}) error {
	m.Lock()
	defer m.Unlock()
	m.WorkerList = append(m.WorkerList, args.WorkerID)
	m.NewWorkerCond.Broadcast()
	return nil
}

//
func (m *Master) MonitorAvailableWorkers(idleWorkerCh chan string) {
	RegisteredWorkerCount := 0
	for {
		m.Lock()
		if len(m.WorkerList) > RegisteredWorkerCount {
			idleWorker := m.WorkerList[RegisteredWorkerCount]
			go func() { idleWorkerCh <- idleWorker }()
			RegisteredWorkerCount++
		} else {
			m.NewWorkerCond.Wait()
		}
		m.Unlock()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	m.L = l
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	//fmt.Println("entering done function")
	ret := <-m.DoneCh

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Files = files
	m.R = nReduce
	m.NewWorkerCond = sync.NewCond(&m)
	m.DoneCh = make(chan bool)
	m.server()
	go m.Run()
	return &m
}

//
func (m *Master) Run() {
	idleWorkerCh := make(chan string, 10)
	go m.MonitorAvailableWorkers(idleWorkerCh)
	m.Schedule("map", idleWorkerCh)
	m.Schedule("reduce", idleWorkerCh)
	m.CleanUp()
	fmt.Println("Clean up completes")
	m.DoneCh <- true
}

//
func (m *Master) CleanUp() {
	m.Lock()
	defer m.Unlock()
	//shutdown workers
	for _, wid := range m.WorkerList {

		//worker might already be dead, then dialing will get connection refuse

		suc := callWorker(wid, "WorkerRPC.Shutdown", new(struct{}), new(struct{}))
		if suc == false {
			fmt.Println("Master cannot shutdown one worker(worker might have crashed early)")
		}
	}
	fmt.Println("shutting down master RPC server")
	//shutdown master RPC
	suc := CallMaster("Master.ShutDown", new(struct{}), new(struct{}))
	if suc == false {
		fmt.Println("Cannot shutdown master RPC server")
	}
	//clean up any intermediate files
	for i := 0; i < len(m.Files); i++ {
		for j := 0; j < m.R; j++ {
			fp := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			os.Remove(fp)
		}
	}
}

//
func (m *Master) ShutDown(_ *struct{}, _ *struct{}) error {
	m.L.Close()
	return nil
}

//
func (m *Master) Schedule(TaskType string, idleWorkerCh chan string) {
	fmt.Printf("%v task scheduling begins\n", TaskType)
	var totalTaskNum int
	switch TaskType {
	case "map":
		totalTaskNum = len(m.Files)
	case "reduce":
		totalTaskNum = m.R
	}
	var wg sync.WaitGroup
	for i := 0; i < totalTaskNum; i++ {
		targs := new(TaskArgs)
		targs.TaskID = i
		targs.TaskType = TaskType
		switch TaskType {
		case "map":
			targs.FilePath = m.Files[i]
			targs.R = m.R
		case "reduce":
			targs.M = len(m.Files)
		}
		wg.Add(1)
		go func(targs *TaskArgs, idx int) {
			defer wg.Done()
		faultTolerate:
			for {
				chosenWorker := <-idleWorkerCh
				resCh := make(chan bool)
				fmt.Printf("executing task %v to worker %v\n", idx, chosenWorker)
				go func(wid string) {
					resCh <- callWorker(wid, "WorkerRPC.RunTask", targs, new(struct{}))
				}(chosenWorker)
				select {
				case res := <-resCh:
					if res == true {
						go func() {
							idleWorkerCh <- chosenWorker
						}()
						break faultTolerate
					}
					continue
				case <-time.After(10 * time.Second):
					continue
				}
			}
		}(targs, i)
	}
	wg.Wait()
	fmt.Printf("%v task scheduling done\n", TaskType)
}

func callWorker(workerID string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.Dial("unix", workerID)
	if err != nil {
		//worker might have crashed early
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
