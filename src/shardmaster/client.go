package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const RetryTime = time.Duration(100 * time.Millisecond)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	me           int64
	requestCount int
	lastLeaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.me = nrand()
	ck.requestCount = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// args := &QueryArgs{}
	// // Your code here.
	// ck.requestCount++
	// args.Num = num
	// args.ClientID = ck.me
	// args.OpID = ck.requestCount
	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply QueryReply
	// 		ok := srv.Call("ShardMaster.Query", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return reply.Config
	// 		}
	// 	}
	// 	time.Sleep(RetryTime)
	// }

	ck.requestCount++
	opID := ck.requestCount
	n := len(ck.servers)
	tryLeaderID := ck.lastLeaderID % n
	DPrintf("Client %v Query request: configuration %v \n", ck.me, num)
	for {
		var qargs QueryArgs
		var qreply QueryReply
		qargs.Num = num
		qargs.OpID = opID
		qargs.ClientID = ck.me
		dest := ck.servers[tryLeaderID%n]
		ok := dest.Call("ShardMaster.Query", &qargs, &qreply)
		// if ok && qreply.WrongLeader == false {
		// 	ck.lastLeaderID = tryLeaderID%n
		// 	return qreply.Config
		// }
		if ok && qreply.Err != ErrWrongLeader {
			switch qreply.Err {
			case OK:
				DPrintf("Client %v Query Success from Server %v:  configuration %v for OpID %v \n", ck.me, tryLeaderID%n, num, opID)
				ck.lastLeaderID = tryLeaderID % n
				return qreply.Config
			case ErrDuplicate:
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				DPrintf("Client %v Query [Duplicate Error] from Server %v:  configuration %v for OpID %v \n", ck.me, tryLeaderID%n, num, opID)
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				ck.lastLeaderID = tryLeaderID % n
				return qreply.Config
			default:
				DPrintf("Client %v Query [%v Error] from Server %v:  configuration %v for OpID %v \n", ck.me, qreply.Err, tryLeaderID%n, num, opID)
			}
		}
		tryLeaderID = (tryLeaderID + 1) % n
		time.Sleep(RetryTime)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// args := &JoinArgs{}
	// // Your code here.
	// ck.requestCount++
	// args.Servers = servers
	// args.ClientID = ck.me
	// args.OpID = ck.requestCount
	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply JoinReply
	// 		ok := srv.Call("ShardMaster.Join", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }

	ck.requestCount++
	opID := ck.requestCount
	n := len(ck.servers)
	tryLeaderID := ck.lastLeaderID % n
	DPrintf("Client %v Join request: servers %v \n", ck.me, servers)
	for {
		var jargs JoinArgs
		var jreply JoinReply
		jargs.Servers = servers
		jargs.OpID = opID
		jargs.ClientID = ck.me
		dest := ck.servers[tryLeaderID%n]
		ok := dest.Call("ShardMaster.Join", &jargs, &jreply)
		// if ok && jreply.WrongLeader == false {
		// 	ck.lastLeaderID = tryLeaderID%n
		// 	return
		// }
		if ok && jreply.Err != ErrWrongLeader {
			switch jreply.Err {
			case OK:
				DPrintf("Client %v Join Success from Server %v: servers %v for OpID %v \n", ck.me, tryLeaderID%n, servers, opID)
				ck.lastLeaderID = tryLeaderID % n
				return
			case ErrDuplicate:
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				DPrintf("Client %v Join [Duplicate Error] from Server %v: servers %d for OpID %v \n", ck.me, tryLeaderID%n, servers, opID)
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				ck.lastLeaderID = tryLeaderID % n
				return
			default:
				DPrintf("Client %v Join [%v Error] from Server %v: servers %d for OpID %v \n", ck.me, jreply.Err, tryLeaderID%n, servers, opID)
			}
		}
		tryLeaderID = (tryLeaderID + 1) % n
		time.Sleep(RetryTime)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// args := &LeaveArgs{}
	// // Your code here.
	// args.GIDs = gids

	// for {
	// 	// try each known server.
	// 	for _, srv := range ck.servers {
	// 		var reply LeaveReply
	// 		ok := srv.Call("ShardMaster.Leave", args, &reply)
	// 		if ok && reply.WrongLeader == false {
	// 			return
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }

	ck.requestCount++
	opID := ck.requestCount
	n := len(ck.servers)
	tryLeaderID := ck.lastLeaderID % n
	DPrintf("Client %v Leave request: groupIDs %v \n", ck.me, gids)
	for {
		var largs LeaveArgs
		var lreply LeaveReply
		largs.GIDs = gids
		largs.OpID = opID
		largs.ClientID = ck.me
		dest := ck.servers[tryLeaderID%n]
		ok := dest.Call("ShardMaster.Leave", &largs, &lreply)
		// if ok && lreply.WrongLeader == false {
		// 	ck.lastLeaderID = tryLeaderID%n
		// 	return
		// }
		if ok && lreply.Err != ErrWrongLeader {
			switch lreply.Err {
			case OK:
				DPrintf("Client %v Leave Success from Server %v: groupIDs %v for OpID %v \n", ck.me, tryLeaderID%n, gids, opID)
				ck.lastLeaderID = tryLeaderID % n
				return
			case ErrDuplicate:
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				DPrintf("Client %v Leave [Duplicate Error] from Server %v: groupIDs %v for OpID %v \n", ck.me, tryLeaderID%n, gids, opID)
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				ck.lastLeaderID = tryLeaderID % n
				return
			default:
				DPrintf("Client %v Leave [%v Error] from Server %v: groupIDs %v for OpID %v \n", ck.me, lreply.Err, tryLeaderID%n, gids, opID)
			}
		}
		tryLeaderID = (tryLeaderID + 1) % n
		time.Sleep(RetryTime)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestCount++
	opID := ck.requestCount
	n := len(ck.servers)
	tryLeaderID := ck.lastLeaderID % n
	DPrintf("Client %v Move request: shard %v, groupID %v \n", ck.me, shard, gid)
	for {
		var margs MoveArgs
		var mreply MoveReply
		margs.Shard = shard
		margs.GID = gid
		margs.OpID = opID
		margs.ClientID = ck.me
		dest := ck.servers[tryLeaderID%n]
		ok := dest.Call("ShardMaster.Move", &margs, &mreply)
		// if ok && mreply.WrongLeader == false {
		// 	ck.lastLeaderID = tryLeaderID%n
		// 	return
		// }
		if ok && mreply.Err != ErrWrongLeader {
			switch mreply.Err {
			case OK:
				DPrintf("Client %v Move Success from Server %v: (shard %v, gid %v) for OpID %v \n", ck.me, tryLeaderID%n, shard, gid, opID)
				ck.lastLeaderID = tryLeaderID % n
				return
			case ErrDuplicate:
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				DPrintf("Client %v Move [Duplicate Error] from Server %v: (shard %v, gid %v) for OpID %v \n", ck.me, tryLeaderID%n, shard, gid, opID)
				DPrintf("************************** FATAL(should rarely see this message) **************************\n")
				ck.lastLeaderID = tryLeaderID % n
				return
			default:
				DPrintf("Client %v Move [%v Error] from Server %v: (shard %v, gid %v) for OpID %v \n", ck.me, mreply.Err, tryLeaderID%n, shard, gid, opID)
			}
		}
		tryLeaderID = (tryLeaderID + 1) % n
		time.Sleep(RetryTime)
	}
}
