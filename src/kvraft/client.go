package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	me int64
	requestCount int
	lastLeaderID int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.requestCount = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestCount++
	opID:= ck.requestCount
	n := len(ck.servers)
	tryLeaderID := ck.lastLeaderID%n
	DPrintf("Client %v Get request: Get Key %v with OpID %v \n", ck.me, key, opID)
	for {
		var gargs GetArgs
		var greply GetReply
		gargs.Key = key
		gargs.OpID = opID
		gargs.ClientID = ck.me
		dest:= ck.servers[tryLeaderID%n]
		ok:= dest.Call("KVServer.Get", &gargs, &greply)
		if ok && greply.Err != ErrWrongLeader {
			switch greply.Err {
			case OK:
				DPrintf("Client %v Get Success from Server %v: Get (Key, Value):  (%v, %v) for OpID %v \n", ck.me, tryLeaderID%n, key, greply.Value, opID)
				ck.lastLeaderID = tryLeaderID%n
				return greply.Value
			case ErrNoKey:
				DPrintf("Client %v Get Error from Server %v: %v for Key %v, OpID %v \n", ck.me, tryLeaderID%n, greply.Err, key, opID)
				ck.lastLeaderID = tryLeaderID%n
				return greply.Value
			case ErrDuplicate:
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				DPrintf("Client %v Get Error from Server %v: %v for Key %v, OpID %v \n", ck.me, tryLeaderID%n, greply.Err, key, opID)
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				ck.lastLeaderID = tryLeaderID%n
				return greply.Value
			default:
				DPrintf("Client %v Get Error from Server %v: %v for Key %v, OpID %v \n", ck.me, tryLeaderID%n, greply.Err, key, opID)
			}
		}
		tryLeaderID=(tryLeaderID+1)%n
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestCount++
	opID:= ck.requestCount
	n := len(ck.servers)
	tryLeaderID := ck.lastLeaderID%n
	DPrintf("Client %v PutAppend request: (Key, Value, opType)=(%v, %v, %v) for OpID %v \n", ck.me, key, value, op, opID)
	for {
		var paargs PutAppendArgs
		var pareply PutAppendReply
		paargs.Key = key
		paargs.Value = value
		paargs.Op = op
		paargs.OpID = opID
		paargs.ClientID = ck.me
		dest := ck.servers[tryLeaderID%n]
		ok := dest.Call("KVServer.PutAppend", &paargs, &pareply)
		if ok && pareply.Err != ErrWrongLeader {
			switch pareply.Err {
			case OK:
				DPrintf("Client %v PutAppend Success from Server %v: (Key, Value, opType)=(%v, %v, %v) for OpID %v \n", ck.me, tryLeaderID%n, key, value, op, opID)
				ck.lastLeaderID = tryLeaderID%n
				return
			case ErrDuplicate:
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				DPrintf("Client %v PutAppend Error from Server %v: %v with (Key, Value, opType)=(%v, %v, %v) and OpID %v \n", ck.me, tryLeaderID%n, pareply.Err, key, value, op, opID)
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				ck.lastLeaderID = tryLeaderID%n
				return
			default:
				DPrintf("Client %v PutAppend Error from Server %v: %v with (Key, Value, opType)=(%v, %v, %v) and OpID %v \n", ck.me, tryLeaderID%n, pareply.Err, key, value, op, opID)
			}
		}
		tryLeaderID=(tryLeaderID+1)%n
	}
}

//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
