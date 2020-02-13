package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"hash/fnv"
	"os"
	"strconv"
	"io/ioutil"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//initialize this worker first
	//use os.Getpid() to assign unique identifier for worker registration
	assignedID := strconv.Itoa(os.Getpid())
	fmt.Println("Worker ", assignedID, "now online")
	w := &WorkerRPC{ID: assignedID, Mapf: mapf, Reducef: reducef, DoneCh: make(chan bool), OtherCh: make(chan bool)}
	wrpc := rpc.NewServer()
	wrpc.Register(w)
	os.Remove(assignedID)
	l, err := net.Listen("unix", assignedID)
	if err != nil {
		log.Fatal("Fail to register the new Worker RPC server with the given worker, get error: ", err)
	}

	//next, let master know this worker
	wrargs := WorkerRegArgs{WorkerID: assignedID}
	//use struct{} since we don't need any reply
	res := CallMaster("Master.RegisterNewWorker", wrargs, new(struct{}))
	if(res == false){
		//maybe we don't need to exit, just print will be fine?
		log.Fatal("Fail to register the master with the given worker, get error: ", err)
	}

	//keep waiting for task from the master
	workerloop:
	for {
		c, err := l.Accept()
		if err == nil {
			//worker should be handling only one RPC at a time only?
			//see later if the schedule method at master can enforce this constraint.
			go wrpc.ServeConn(c)
			select{
			case <- w.DoneCh:
				break workerloop
			case <- w.OtherCh:
				continue
			}
		}else{
			fmt.Println("Worker fails to get the request from the master")
			break
		}
	}
	l.Close()
	fmt.Println("Worker ", assignedID, "now offline")
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//
func (w *WorkerRPC) Shutdown(_ *struct{}, _ *struct{}) error{
	w.DoneCh <- true
	return nil
}

// RunTask will be called by the master
func (w *WorkerRPC) RunTask(args *TaskArgs, _ *struct{}) error {
	switch args.TaskType {
	case "map":
		RunMapTask(args.TaskID, args.FilePath, args.R, w.Mapf)
	case "reduce":
		RunReduceTask(args.TaskID, args.M, w.Reducef)
	default:
		log.Fatal("Unknown task type for worker ", w.ID)
	}
	//if worker crashes before reaching this line, master will never get response and get timed out
	w.OtherCh<-true
	return nil
}

//RunMapTask will run the mapf function with given input
func RunMapTask(taskID int, filePath string, R int, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal("Cannot read the file: ", filePath)
	}
	res := mapf(filePath, string(content[:]))
	tempEncoderMap := make(map[string]*json.Encoder)
	for i:=0;i<len(res);i++ {
		partitionID := ihash(res[i].Key) % R
		partitionFileName := "mr-"+strconv.Itoa(taskID)+"-"+strconv.Itoa(partitionID)
		tempEncoder, exists := tempEncoderMap[partitionFileName]
		if exists == false {
			newTempFile, err := ioutil.TempFile(os.TempDir(), "mr-")
			if err != nil {
				log.Fatal("Cannot create new temporary file: ", err)
			}
			defer func(){
				//close the file and atomically rename it
				newTempFile.Close()
				os.Rename(newTempFile.Name(), partitionFileName)
				//fmt.Println("file written and renamed")
			}()
			tempEncoderMap[partitionFileName] = json.NewEncoder(newTempFile)
			tempEncoder = tempEncoderMap[partitionFileName]
		}
		//fmt.Println(res[i])
		err := tempEncoder.Encode(&res[i])
		if err != nil {
			log.Fatal("Cannot write this key value pair: ", err)
		}
	}
	return
}

//
func RunReduceTask(taskID int, M int, reducef func(string, []string) string){
	intermediate := []KeyValue{}
	for i:=0;i<M;i++ {
		targetFilePath := "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(taskID)
		targetFile, err := os.Open(targetFilePath)
		if err != nil {
			//cannot use log.fatal here otherwise map/reduce parallel test fails?
			fmt.Println("Fail to open this file")
		}
		defer targetFile.Close()
		dec := json.NewDecoder(targetFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"+strconv.Itoa(taskID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	fmt.Println("file written")
	ofile.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	CallMaster("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func CallMaster(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
