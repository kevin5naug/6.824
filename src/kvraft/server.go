package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

//
const Debug = 1

//
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[KVServer] "+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Mode     string
	Key      string
	Value    string
	ClientID int64
	OpID     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Database          map[string]string
	LastServiceRecord map[int64]int //clientID to opID
	PendingOps        map[int]chan Op
	// reason why we need to cache and when we use it:
	// client 1 sends a get request, server accepts it and has added it to the log
	// but the request gets timed out before server send the get results to PendingOps,
	// and after server executed the get request
}

//
func (kv *KVServer) AddPendingOps(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.PendingOps[idx]
	if !ok {
		ch = make(chan Op, 1) //non blocking
		kv.PendingOps[idx] = ch
	}
	return ch
}

//
func (kv *KVServer) MonitorAndApplyPendingOps() {
	for {
		kv.mu.Lock()
		if kv.dead == 1 {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		select {
		case logEntry := <-kv.applyCh:
			if !logEntry.CommandValid {
				serverData := logEntry.Command.([]byte)
				kv.mu.Lock()
				kv.setSnapshotData(serverData)
				DPrintf("RESET STATE: server %d has reset its state\n", kv.me)
				kv.mu.Unlock()
				continue
			}
			if logEntry.Command == nil {
				//no-op
				kv.mu.Lock()
				DPrintf("Server %v last applied log index: %d\n", kv.me, logEntry.CommandIndex-1)
				kv.checkNeedsForSnapshot(logEntry.CommandIndex - 1)
				kv.mu.Unlock()
				continue
			}
			op := logEntry.Command.(Op)
			kv.mu.Lock()
			DPrintf("Server %v last applied log index: %d\n", kv.me, logEntry.CommandIndex-1)
			lastOpID, ok := kv.LastServiceRecord[op.ClientID]
			if !ok || op.OpID > lastOpID {
				switch op.Mode {
				case "Get":
					op.Value = kv.Database[op.Key]
				case "Put":
					kv.Database[op.Key] = op.Value
				case "Append":
					kv.Database[op.Key] += op.Value
					DPrintf("Server %v executing latest append request for client %v: (key: %v, value: %v, OpID: %v)\n", kv.me, op.ClientID, op.Key, op.Value, op.OpID)
				}
				kv.LastServiceRecord[op.ClientID] = op.OpID
			}
			if op.Mode == "Get" {
				//it is possible that multiple new get requests from the same client with the same OpID get to this point at the same time
				//the first one will get the value, but the rest of them should have value too
				op.Value = kv.Database[op.Key]
			}
			ch, ok := kv.PendingOps[logEntry.CommandIndex]
			if ok {
				//request not timed out yet
				ch <- op
			}
			kv.checkNeedsForSnapshot(logEntry.CommandIndex - 1)
			kv.mu.Unlock()
		case <-time.After(1000 * time.Millisecond):
			//check whether this server is killed again
			continue
		}
	}
}

//
func (kv *KVServer) DeletePendingOps(idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.PendingOps, idx)
}

func (kv *KVServer) checkNeedsForSnapshot(logIdx int) {
	//this function holds the lock of kvserver
	//DPrintf("Server %d checkNeedsForSnapshot receives logIdx: %d\n", kv.me, logIdx)
	if kv.maxraftstate == -1 {
		//snapshot disabled
		return
	}
	curRaftStateSize := kv.rf.GetRaftStateSize()
	if curRaftStateSize < kv.maxraftstate*8/10 {
		DPrintf("Server %d finds snapshot is not needed for logIdx: %d, current raft state size is %d, but the threshold is %d \n", kv.me, logIdx, curRaftStateSize, kv.maxraftstate*8/10)
		return
	}
	serverStates := kv.encodeData()
	DPrintf("Server %d finds snapshot is needed. Going to discard logs with log index up to(including): %d\n", kv.me, logIdx)
	go kv.rf.TakeSnapshot(logIdx, serverStates)
}

//
func (kv *KVServer) encodeData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Database)
	e.Encode(kv.LastServiceRecord)
	return w.Bytes()
}

//
func (kv *KVServer) setSnapshotData(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var database map[string]string
	var lastServiceRecord map[int64]int //clientID to opID
	if d.Decode(&database) != nil || d.Decode(&lastServiceRecord) != nil {
		DPrintf("ERROR: Server %d fails to read from persist data\n", kv.me)
	} else {
		kv.Database = database
		kv.LastServiceRecord = lastServiceRecord
	}
}

//
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// //if too many logs uncommitted, just reject requests, let clients try later
	// curStateSize := kv.rf.GetRaftStateSize()
	// if curStateSize >= kv.maxraftstate {
	// 	reply.Err = ErrBusy
	// 	time.Sleep(1 * time.Second)
	// 	return
	// }
	kv.mu.Lock()
	//DPrintf("server %d current state size: %d is not approaching snapshot threshold\n", kv.me, curStateSize)
	lastOpID, ok := kv.LastServiceRecord[args.ClientID]
	if ok && lastOpID >= args.OpID {
		//client won't make the next request unless the previous request has been served
		//we must have already served this request before
		DPrintf("Server detect duplicate get request detected: (key: %v, OpID: %v, ClientID: %v) while serving OpID %v... \n", args.Key, args.OpID, args.ClientID, lastOpID)
		reply.Err = ErrDuplicate
		reply.Value = kv.Database[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	var command Op
	command.Mode = "Get"
	command.Key = args.Key
	command.Value = ""
	command.OpID = args.OpID
	command.ClientID = args.ClientID
	// idx, _, isLeader := kv.rf.Start(command)
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	idx, _, isLeader, isBusy := kv.rf.StartIfSpaceAllow(command, kv.maxraftstate)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if isBusy {
		reply.Err = ErrBusy
		time.Sleep(1 * time.Second)
		return
	}
	ch := kv.AddPendingOps(idx)
	defer kv.DeletePendingOps(idx)
	select {
	case op := <-ch:
		//op is committed and applied before timeout
		if command.Mode == op.Mode && command.Key == op.Key && command.OpID == op.OpID && command.ClientID == op.ClientID {
			reply.Value = op.Value
			reply.Err = OK
			return
		}
		//if we reach this line, other op is committed at this index
		reply.Err = ErrOpMismatch
		return
	case <-time.After(1000 * time.Millisecond):
		//time out
		reply.Err = ErrTimeOut
		return
	}
}

//
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// curStateSize := kv.rf.GetRaftStateSize()
	// if curStateSize >= kv.maxraftstate {
	// 	reply.Err = ErrBusy
	// 	time.Sleep(1 * time.Second)
	// 	return
	// }
	kv.mu.Lock()
	//DPrintf("server %d current state size: %d is not approaching snapshot threshold\n", kv.me, curStateSize)
	lastOpID, ok := kv.LastServiceRecord[args.ClientID]
	if ok && lastOpID >= args.OpID {
		//client won't make the next request unless the previous request has been served
		//we must have already served this request before
		DPrintf("Server detect duplicate PutAppend request detected: (key: %v, value: %v, OpID: %v, ClientID: %v) while serving OpID %v... \n", args.Key, args.Value, args.OpID, args.ClientID, lastOpID)
		reply.Err = ErrDuplicate
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	var command Op
	command.Mode = args.Op
	command.Key = args.Key
	command.Value = args.Value
	command.OpID = args.OpID
	command.ClientID = args.ClientID
	// idx, _, isLeader := kv.rf.Start(command)
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	idx, _, isLeader, isBusy := kv.rf.StartIfSpaceAllow(command, kv.maxraftstate)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if isBusy {
		reply.Err = ErrBusy
		time.Sleep(1 * time.Second)
		return
	}
	ch := kv.AddPendingOps(idx)
	defer kv.DeletePendingOps(idx)
	select {
	case op := <-ch:
		//op is committed and applied before timeout
		if command.Mode == op.Mode && command.Key == op.Key && command.Value == op.Value && command.OpID == op.OpID && command.ClientID == op.ClientID {
			reply.Err = OK
			return
		}
		//if we reach this line, other op is committed at this index
		reply.Err = ErrOpMismatch
		return
	case <-time.After(1000 * time.Millisecond):
		//time out
		reply.Err = ErrTimeOut
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	//kv.maxraftstate = 1
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0
	// You may need initialization code here.
	kv.Database = make(map[string]string)
	kv.LastServiceRecord = make(map[int64]int) //clientID to opID
	kv.PendingOps = make(map[int]chan Op)
	serverData := persister.ReadSnapshot()
	kv.setSnapshotData(serverData)
	DPrintf("RESTART: server %d is reading from its persisted file \n", kv.me)
	go kv.MonitorAndApplyPendingOps()

	return kv
}
