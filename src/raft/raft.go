package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persist on all servers
	currentTerm int
	votedFor    int
	logList     []LogEntry

	//volatile on all servers
	commitIndex int
	lastApplied int

	//volatile on leaders
	nextIndex  []int
	matchIndex []int

	state                     string //whether it is a leader, follower or candidate
	found_leader_or_candidate bool
	rng                       *rand.Rand
	voteCount                 int
	applyCh                   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logList)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logList []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logList) != nil {
		DPrintf("ERROR: Machine %d fails to read from persist data\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logList = logList
	}
}

//
type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
type AppendEntryReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	isValidCandidate := true
	if len(rf.logList) > 0 {
		lastIdx := len(rf.logList) - 1
		if args.LastLogTerm < rf.logList[lastIdx].Term || (args.LastLogTerm == rf.logList[lastIdx].Term && args.LastLogIndex < lastIdx) {
			isValidCandidate = false
		}
	}
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		if isValidCandidate && rf.votedFor == -1 {
			rf.votedFor = args.CandidateID
			rf.persist()
			DPrintf("ELECTION VOTE NEWS: candidate machine %v has received vote from machine %v for term %d!\n", args.CandidateID, rf.me, rf.currentTerm)
			rf.found_leader_or_candidate = true
		}
		if rf.votedFor == args.CandidateID {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		return
	}
	//at this point, we must have args.Term > rf.currentTerm
	rf.state = "follower"
	rf.currentTerm = args.Term
	rf.votedFor = -1
	if isValidCandidate {
		rf.votedFor = args.CandidateID
		rf.found_leader_or_candidate = true
	}
	rf.persist()
	DPrintf("ELECTION VOTE NEWS: candidate machine %v has received vote from machine %v for term %d!\n", args.CandidateID, rf.me, rf.currentTerm)
	reply.Term = args.Term
	if rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		isLeader = false
		return index, term, isLeader
	}

	//leader appends the log and send out appendEntry request to followers
	DPrintf("*************************************\n")
	DPrintf("Leader machine %d has received one log{command: %v, writtenTerm: %v} from the client\n", rf.me, command, rf.currentTerm)
	DPrintf("*************************************\n")
	rf.logList = append(rf.logList, LogEntry{command, rf.currentTerm})
	rf.persist()
	index = len(rf.logList) //should we put -1 here???
	term = rf.currentTerm
	return index, term, isLeader

	// use the heartbeat message to send out the logs
	// n := len(rf.peers)
	// appendSuccessCount := 1
	// for i:=0;i<n;i++{
	// 	if i==rf.me {
	// 		continue
	// 	}
	// 	var args AppendEntryArgs
	// 	args.Term = rf.currentTerm
	// 	args.LeaderID = rf.me
	// 	args.PrevLogIndex = rf.nextIndex[i]-1
	// 	if args.PrevLogIndex >= 0 {
	// 		args.PrevLogTerm = rf.logList[args.PrevLogIndex].Term
	// 	}
	// 	if rf.nextIndex[i] < len(rf.logList) {
	// 		args.Entries = rf.logList[rf.nextIndex[i]:]
	// 	}
	// 	args.LeaderCommit = rf.commitIndex
	// 	go func(peerID int, args AppendEntryArgs){
	// 		var reply AppendEntryReply
	// 	}(i, args)
	// }
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logList = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = "follower"
	//rf.rng = rand.New(rand.NewSource(int64(me)))
	rf.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.voteCount = 0
	rf.found_leader_or_candidate = false
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	go rf.handleTimeOut()

	return rf
}

func (rf *Raft) handleTimeOut() {
	magicNumber := 725
	for {
		electionTimeout := magicNumber + rf.rng.Int()%magicNumber //cannot be either too large or too small
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.dead == 1 {
			//this machine is dead, no need to loop anymore
			rf.mu.Unlock()
			return
		}
		if rf.state != "leader" && rf.found_leader_or_candidate == false {
			//reason why "rf.state == "follower" is not enough:
			//we might have two candidates in an election
			//but fail to produce a leader
			rf.startElectionCampaign()
			rf.mu.Unlock()
			continue
		}
		rf.found_leader_or_candidate = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElectionCampaign() {
	//don't lock here, otherwise will result in deadlock!!!
	//startElectionCampaign will only be invoked by handleTimeout
	//and handleTimeout will have the lock for this machine
	DPrintf("ELECTION BEGINS: machine %d has decided to start an election campaign at term %d !\n", rf.me, rf.currentTerm)
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	rf.found_leader_or_candidate = true
	n := len(rf.peers)
	rvargs := &RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: len(rf.logList) - 1}
	if len(rf.logList) > 0 {
		rvargs.LastLogTerm = rf.logList[len(rf.logList)-1].Term
	}
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		go func(serverIdx int, rvargs *RequestVoteArgs) {
			var rvreply RequestVoteReply
			ok := rf.sendRequestVote(serverIdx, rvargs, &rvreply)
			if ok {
				rf.mu.Lock()
				if rvreply.Term > rf.currentTerm {
					//we have found a leader with higher term
					rf.currentTerm = rvreply.Term
					rf.state = "follower"
					rf.votedFor = -1
					rf.persist()
				} else if rvreply.Term == rf.currentTerm && rvreply.VoteGranted == true {
					rf.voteCount++
					if rf.voteCount > n/2 {
						if rf.state == "candidate" {
							DPrintf("ELECTION RESULT: machine %d is now a leader for term %d !\n", rf.me, rf.currentTerm)
							rf.state = "leader"
							for peerID := 0; peerID < len(rf.peers); peerID++ {
								if peerID == rf.me {
									continue
								}
								rf.nextIndex[peerID] = len(rf.logList)
								rf.matchIndex[peerID] = -1
							}
							go rf.advertiseLeaderStatus()
						}
					}
				}
				rf.mu.Unlock()
			}
		}(i, rvargs)

	}
}

func (rf *Raft) advertiseLeaderStatus() {
	magicNumber := 150
	for {
		rf.mu.Lock()
		if rf.state != "leader" || rf.dead != 0 {
			rf.mu.Unlock()
			return
		}
		n := len(rf.peers)
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			var aeargs AppendEntryArgs
			aeargs.Term = rf.currentTerm
			aeargs.LeaderID = rf.me
			aeargs.PrevLogIndex = rf.nextIndex[i] - 1
			if aeargs.PrevLogIndex >= 0 {
				aeargs.PrevLogTerm = rf.logList[aeargs.PrevLogIndex].Term
			}
			if rf.nextIndex[i] < len(rf.logList) {
				aeargs.Entries = rf.logList[rf.nextIndex[i]:]
			} else {
				aeargs.Entries = nil
			}
			aeargs.LeaderCommit = rf.commitIndex
			go func(peerID int, aeargs AppendEntryArgs) {
				var aereply AppendEntryReply
				ok := rf.sendAppendEntry(peerID, &aeargs, &aereply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != "leader" {
						return
					}
					if aereply.Term > rf.currentTerm {
						rf.currentTerm = aereply.Term
						rf.state = "follower"
						rf.votedFor = -1
						rf.persist()
						return
					} else if aereply.Term == rf.currentTerm {
						if aereply.Success {
							rf.nextIndex[peerID] = aereply.MatchIndex + 1
							rf.matchIndex[peerID] = aereply.MatchIndex
							if rf.commitIndex < rf.matchIndex[peerID] && rf.logList[rf.matchIndex[peerID]].Term == rf.currentTerm {
								//only current term can commit by counting
								successCount := 1
								n := len(rf.peers)
								for i := 0; i < n; i++ {
									if i == rf.me {
										continue
									}
									if rf.matchIndex[i] >= rf.matchIndex[peerID] {
										successCount++
									}
								}
								if successCount > n/2 {
									rf.commitIndex = rf.matchIndex[peerID]
									go rf.applyLogs()
								}
							}
						} else {
							//appendEntry fails
							if aereply.MatchIndex == -1 {
								//no log in PrevLogTerm
								curPreIndex := aeargs.PrevLogIndex - 1
								if aeargs.PrevLogIndex <= -1 {
									curPreIndex = -1
								}
								for curPreIndex >= 0 && rf.logList[curPreIndex].Term >= aeargs.PrevLogTerm {
									curPreIndex--
								}
								rf.nextIndex[peerID] = curPreIndex + 1
							} else {
								rf.nextIndex[peerID] = aereply.MatchIndex + 1
							}
							//rf.nextIndex[peerID] = aereply.MatchIndex + 1
						}
					} else {
						//do nothing
					}
				}
			}(i, aeargs)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(magicNumber) * time.Millisecond)
	}
}

//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf("ELECTION VOTE RESET: machine %d have updated its current term to %d and can still vote for that term!\n", rf.me, rf.currentTerm)
	}
	rf.state = "follower"
	rf.found_leader_or_candidate = true
	reply.Term = args.Term
	if args.PrevLogIndex >= 0 && (len(rf.logList) <= args.PrevLogIndex || rf.logList[args.PrevLogIndex].Term != args.PrevLogTerm) {
		DPrintf("APPENDENTRY: machine %d finds a prev log mismatch from leader machine %v at term %d !\n", rf.me, args.LeaderID, rf.currentTerm)
		DPrintf("PrevLogIndex: %d, len(rf.logList): %d\n", args.PrevLogIndex, len(rf.logList))
		if len(rf.logList) > args.PrevLogIndex {
			DPrintf("rf.logList[args.PrevLogIndex].Term: %d, args.Term: %d \n", rf.logList[args.PrevLogIndex].Term, args.Term)
		}
		//we can't find prevLog
		//heart beat message can reach here if:
		// 1) a follower crashed and missed some logs
		// 2) the follower becomes online again
		matchIndex := len(rf.logList) - 1
		if matchIndex > args.PrevLogIndex {
			matchIndex = args.PrevLogIndex
		}
		// find the first index it stores for that term
		for matchIndex >= 0 {
			if rf.logList[matchIndex].Term == args.PrevLogTerm {
				break
			}
			if rf.logList[matchIndex].Term < args.PrevLogTerm {
				matchIndex = -1
				break
			}
			matchIndex--
		}
		reply.Success = false
		reply.MatchIndex = matchIndex
	} else if args.Entries != nil {
		DPrintf("APPENDENTRY: machine %d adding new logs from leader machine %v at term %d !\n", rf.me, args.LeaderID, rf.currentTerm)
		//we can find the prevLog
		//should not truncate the log !!!
		n := len(args.Entries)
		for i := 1; i <= n; i++ {
			if args.PrevLogIndex+i < len(rf.logList) && rf.logList[args.PrevLogIndex+i].Term != args.Entries[i-1].Term {
				rf.logList = rf.logList[:args.PrevLogIndex+i] //truncate the log
				break                                         //all following logs deleted, no need to loop anymore
			}
		}
		for i := 1; i <= n; i++ {
			if args.PrevLogIndex+i < len(rf.logList) {
				rf.logList[args.PrevLogIndex+i] = args.Entries[i-1]
			} else {
				rf.logList = append(rf.logList, args.Entries[i-1])
			}
		}
		// rf.logList = rf.logList[:args.PrevLogIndex+1]
		// rf.logList = append(rf.logList, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if args.LeaderCommit >= args.PrevLogIndex+n {
				rf.commitIndex = args.PrevLogIndex + n
			}
			go rf.applyLogs()
		}
		reply.Success = true
		reply.MatchIndex = args.PrevLogIndex + n
		DPrintf("PrevLogIndex: %d, len(rf.logList): %d\n", args.PrevLogIndex, len(rf.logList))
	} else {
		DPrintf("APPENDENTRY: machine %d receives a heart beat msg from leader machine %v at term %d !\n", rf.me, args.LeaderID, rf.currentTerm)
		//heart beat
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if args.LeaderCommit >= args.PrevLogIndex {
				rf.commitIndex = args.PrevLogIndex
			}
			go rf.applyLogs()
		}
		reply.Success = true
		reply.MatchIndex = args.PrevLogIndex
		DPrintf("PrevLogIndex: %d, len(rf.logList): %d\n", args.PrevLogIndex, len(rf.logList))
	}
	rf.persist()
	//DPrintf("machine %d should not reach this step in AppendEntry in 2A at term %d !\n", rf.me, rf.currentTerm)
	//return
}

//
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//sanity check
	if rf.commitIndex > len(rf.logList)-1 {
		rf.commitIndex = len(rf.logList) - 1
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		DPrintf("COMMITLOG: machine %d is going to commit log index %d: {command %v written term %v} at term %d\n", rf.me, i, rf.logList[i].Command, rf.logList[i].Term, rf.currentTerm)
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		//DPrintf("COMMITLOG: machine %d is going to commit log index %d: {command %v written term %v} at term %d\n", rf.me, i, rf.logList[i].Command, rf.logList[i].Term, rf.currentTerm)
		rf.applyCh <- ApplyMsg{CommandIndex: i + 1, Command: rf.logList[i].Command, CommandValid: true}
	}
	//just make sure we are not going back, maybe not needed?
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied = rf.commitIndex
	}
}
