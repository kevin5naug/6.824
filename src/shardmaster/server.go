package shardmaster

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

//
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[ShardMaster] "+format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxraftstate      int           // snapshot if log grows this big
	configs           []Config      // indexed by config num
	LastServiceRecord map[int64]int //clientID to opID
	PendingOps        map[int]chan Op
	dead              int32 // set by Kill()
}

type Op struct {
	// Your data here.
	Mode     string
	Args     interface{}
	ClientID int64
	OpID     int
	Cfg      Config
}

func (sm *ShardMaster) createNextConfig() Config {
	//hold the lock of sm when entering this function
	target := sm.configs[len(sm.configs)-1]
	var ncfg Config
	ncfg.Num = target.Num + 1
	ncfg.Shards = target.Shards
	ncfg.Groups = make(map[int][]string)
	for groupID, serverList := range target.Groups {
		ncfg.Groups[groupID] = append([]string{}, serverList...)
	}
	return ncfg
}

func (sm *ShardMaster) balanceNewGroup(cfg *Config, receiverGID int) {
	//hold the lock of sm when entering this function
	groupShardListMap := map[int][]int{}
	for gID := range cfg.Groups {
		groupShardListMap[gID] = []int{}
	}
	for shardIdx, gID := range cfg.Shards {
		groupShardListMap[gID] = append(groupShardListMap[gID], shardIdx)
	}
	shardsPerGroup := NShards / len(cfg.Groups)
	for i := 0; i < shardsPerGroup; i++ {
		//implement a heap for shardsGroupMap would be better?
		//find the donorGID with maxed size first
		curMax := -1
		var donorGID int
		for gID, shardList := range groupShardListMap {
			if curMax < len(shardList) {
				curMax = len(shardList)
				donorGID = gID
			}
		}
		//give the first shard in the shard list of donorGID to receiverGID
		cfg.Shards[groupShardListMap[donorGID][0]] = receiverGID
		groupShardListMap[donorGID] = groupShardListMap[donorGID][1:]
	}
}

func (sm *ShardMaster) executeJoin(op Op) {
	//hold the lock of sm when entering this function
	ncfg := sm.createNextConfig()
	jargs := op.Args.(JoinArgs)
	for groupID, serverList := range jargs.Servers {
		//create copy first
		serverListCopy := make([]string, len(serverList))
		copy(serverListCopy, serverList)
		ncfg.Groups[groupID] = serverListCopy
		sm.balanceNewGroup(&ncfg, groupID)
	}
	sm.configs = append(sm.configs, ncfg)
}

func (sm *ShardMaster) balanceDeletedGroup(cfg *Config, donorID int) {
	//hold the lock of sm when entering this function
	groupShardListMap := map[int][]int{}
	for gID := range cfg.Groups {
		groupShardListMap[gID] = []int{}
	}
	for shardIdx, gID := range cfg.Shards {
		groupShardListMap[gID] = append(groupShardListMap[gID], shardIdx)
	}
	donorShardList, ok := groupShardListMap[donorID]
	if !ok {
		//sanity check: this donorID has already left the group
		return
	}
	delete(groupShardListMap, donorID)
	for _, shardIdx := range donorShardList {
		//find the receiver gID with minimal size
		curMin := -1
		var receiverGID int
		for gID, shardList := range groupShardListMap {
			if curMin == -1 {
				curMin = len(shardList)
				receiverGID = gID
			} else if curMin > len(shardList) {
				curMin = len(shardList)
				receiverGID = gID
			}
		}
		cfg.Shards[shardIdx] = receiverGID
		groupShardListMap[receiverGID] = append(groupShardListMap[receiverGID], shardIdx)
	}
}

func (sm *ShardMaster) executeLeave(op Op) {
	//hold the lock of sm when entering this function
	ncfg := sm.createNextConfig()
	largs := op.Args.(LeaveArgs)
	for _, groupID := range largs.GIDs {
		delete(ncfg.Groups, groupID)
		sm.balanceDeletedGroup(&ncfg, groupID)
	}
	sm.configs = append(sm.configs, ncfg)
}

func (sm *ShardMaster) executeMove(op Op) {
	ncfg := sm.createNextConfig()
	margs := op.Args.(MoveArgs)
	targetShardIdx := margs.Shard
	targetGroupID := margs.GID
	_, ok := ncfg.Groups[targetGroupID]
	if !ok {
		//sanity check
		DPrintf("current ncfg: %v\n", ncfg)
		DPrintf("targetGroupID: %v\n", targetGroupID)
		log.Fatalf("!!!executeMove sanity check failed!!!")
		return
	}
	ncfg.Shards[targetShardIdx] = targetGroupID
	sm.configs = append(sm.configs, ncfg)
}

//
func (sm *ShardMaster) AddPendingOps(idx int) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ch, ok := sm.PendingOps[idx]
	if !ok {
		ch = make(chan Op, 1) //non blocking
		sm.PendingOps[idx] = ch
	}
	return ch
}

//
func (sm *ShardMaster) MonitorAndApplyPendingOps() {
	for {
		sm.mu.Lock()
		if sm.dead == 1 {
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		select {
		case logEntry := <-sm.applyCh:
			if logEntry.Command == nil {
				//no-op
				sm.mu.Lock()
				DPrintf("Server %v last applied log index: %d\n", sm.me, logEntry.CommandIndex-1)
				sm.mu.Unlock()
				continue
			}
			op := logEntry.Command.(Op)
			sm.mu.Lock()
			DPrintf("Server %v last applied log index: %d\n", sm.me, logEntry.CommandIndex-1)
			lastOpID, ok := sm.LastServiceRecord[op.ClientID]
			if !ok || op.OpID > lastOpID {
				switch op.Mode {
				case "Query":
					// qargs := op.Args.(QueryArgs)
					// if qargs.Num >= 0 && qargs.Num < len(sm.configs) {
					// 	op.Cfg = sm.configs[qargs.Num]
					// } else {
					// 	op.Cfg = sm.configs[len(sm.configs)-1]
					// }
				case "Join":
					sm.executeJoin(op)
				case "Leave":
					sm.executeLeave(op)
				case "Move":
					sm.executeMove(op)
				}
				sm.LastServiceRecord[op.ClientID] = op.OpID
			}
			// if op.Mode == "Query" {
			// 	//it is possible that multiple new get requests from the same client with the same OpID get to this point at the same time
			// 	//the first one will get the value, but the rest of them should have value too
			// 	qargs := op.Args.(QueryArgs)
			// 	if qargs.Num >= 0 && qargs.Num < len(sm.configs) {
			// 		op.Cfg = sm.configs[qargs.Num]
			// 	} else {
			// 		op.Cfg = sm.configs[len(sm.configs)-1]
			// 	}
			// }
			ch, ok := sm.PendingOps[logEntry.CommandIndex]
			if ok {
				//request not timed out yet
				ch <- op
			}
			sm.mu.Unlock()
		case <-time.After(1000 * time.Millisecond):
			//check whether this server is killed again
			continue
		}
	}
}

//
func (sm *ShardMaster) DeletePendingOps(idx int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.PendingOps, idx)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var command Op
	command.Mode = "Join"
	command.Args = *args
	command.ClientID = args.ClientID
	command.OpID = args.OpID
	status := sm.UniversalOPHandler(command)
	reply.Err = status
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var command Op
	command.Mode = "Leave"
	command.Args = *args
	command.ClientID = args.ClientID
	command.OpID = args.OpID
	status := sm.UniversalOPHandler(command)
	reply.Err = status
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var command Op
	command.Mode = "Move"
	command.Args = *args
	command.ClientID = args.ClientID
	command.OpID = args.OpID
	status := sm.UniversalOPHandler(command)
	reply.Err = status
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	//Your code here.
	var command Op
	command.Mode = "Query"
	command.Args = *args
	command.ClientID = args.ClientID
	command.OpID = args.OpID
	status := sm.UniversalOPHandler(command)
	if status == OK {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}
	reply.Err = status

	// _, isLeader := sm.rf.GetState()
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	// sm.mu.Lock()
	// //DPrintf("server %d current state size: %d is not approaching snapshot threshold\n", kv.me, curStateSize)
	// lastOpID, ok := sm.LastServiceRecord[args.ClientID]
	// if ok && lastOpID >= args.OpID {
	// 	//client won't make the next request unless the previous request has been served
	// 	//we must have already served this request before
	// 	DPrintf("Server detect duplicate query request: (configuration: %v, ClientID: %v) while serving OpID %v... \n", args.Num, args.ClientID, lastOpID)
	// 	reply.Err = ErrDuplicate
	// 	if args.Num >= 0 && args.Num < len(sm.configs) {
	// 		reply.Config = sm.configs[args.Num]
	// 	} else {
	// 		reply.Config = sm.configs[len(sm.configs)-1]
	// 	}
	// 	sm.mu.Unlock()
	// 	return
	// }
	// sm.mu.Unlock()
	// var command Op
	// command.Mode = "Query"
	// command.Args = *args
	// command.OpID = args.OpID
	// command.ClientID = args.ClientID
	// // idx, _, isLeader := kv.rf.Start(command)
	// // if !isLeader {
	// // 	reply.Err = ErrWrongLeader
	// // 	return
	// // }
	// idx, _, isLeader, isBusy := sm.rf.StartIfSpaceAllow(command, sm.maxraftstate)
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	// if isBusy {
	// 	reply.Err = ErrBusy
	// 	time.Sleep(1 * time.Second)
	// 	return
	// }
	// ch := sm.AddPendingOps(idx)
	// defer sm.DeletePendingOps(idx)
	// select {
	// case op := <-ch:
	// 	//op is committed and applied before timeout
	// 	if command.Mode == op.Mode && command.OpID == op.OpID && command.ClientID == op.ClientID {
	// 		DPrintf("Server successfully served query request: (configuration: %v, ClientID: %v)... \n", args.Num, args.ClientID)
	// 		reply.Config = op.Cfg
	// 		reply.Err = OK
	// 		return
	// 	}
	// 	//if we reach this line, other op is committed at this index
	// 	reply.Err = ErrOpMismatch
	// 	return
	// case <-time.After(1000 * time.Millisecond):
	// 	//time out
	// 	reply.Err = ErrTimeOut
	// 	return
	// }
}

func (sm *ShardMaster) UniversalOPHandler(command Op) Err {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		return ErrWrongLeader
	}
	sm.mu.Lock()
	lastOpID, ok := sm.LastServiceRecord[command.ClientID]
	if ok && lastOpID >= command.OpID {
		//client won't make the next request unless the previous request has been served
		//we must have already served this request before
		DPrintf("ShardMaster %v detect duplicate request: (Mode: %v, Args: %v, ClientID: %v, OpID: %v) while serving OpID %v... \n", sm.me, command.Mode, command.Args, command.ClientID, command.OpID, lastOpID)
		sm.mu.Unlock()
		return ErrDuplicate
	}
	sm.mu.Unlock()
	idx, _, isLeader, isBusy := sm.rf.StartIfSpaceAllow(command, sm.maxraftstate)
	if !isLeader {
		return ErrWrongLeader
	}
	if isBusy {
		time.Sleep(1 * time.Second)
		return ErrBusy
	}
	ch := sm.AddPendingOps(idx)
	defer sm.DeletePendingOps(idx)
	select {
	case op := <-ch:
		//op is committed and applied before timeout
		if command.Mode == op.Mode && command.OpID == op.OpID && command.ClientID == op.ClientID {
			DPrintf("Server %d successfully served ClientID: %v 's request : (mode: %v, args: %v, OpID: %v)... \n", sm.me, op.ClientID, op.Mode, op.Args, op.OpID)
			return OK
		}
		//if we reach this line, other op is committed at this index
		return ErrOpMismatch
	case <-time.After(1000 * time.Millisecond):
		//time out
		return ErrTimeOut
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(MoveArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.dead = 0
	sm.maxraftstate = -1
	// You may need initialization code here.
	sm.LastServiceRecord = make(map[int64]int) //clientID to opID
	sm.PendingOps = make(map[int]chan Op)
	go sm.MonitorAndApplyPendingOps()
	return sm
}
