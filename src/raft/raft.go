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
	applyCond                 *sync.Cond

	//lab3B
	lastIncludedIndex int //persistent in snapshot
	lastIncludedTerm  int //persistent in snapshot
}

//realIdx: real index of the log
//logIdx: current index in the logList
//todo(might cause bugs here): make sure the following functions are executed with locks on raft instance
func (rf *Raft) getLog(realIdx int) LogEntry {
	return rf.logList[realIdx-rf.lastIncludedIndex-1]
}

func (rf *Raft) getRealIdx(logIdx int) int {
	return logIdx + rf.lastIncludedIndex + 1
}

func (rf *Raft) getLogIdx(realIdx int) int {
	return realIdx - rf.lastIncludedIndex - 1
}

func (rf *Raft) getLastRealIdx() int {
	return len(rf.logList) - 1 + rf.lastIncludedIndex + 1
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logList) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logList[len(rf.logList)-1].Term
}

func (rf *Raft) getRealLogLength() int {
	return rf.getLastRealIdx() + 1
}

//
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logList)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logList) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("ERROR: Machine %d fails to read from persist data\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logList = logList
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	if rf.getRealLogLength() > 0 {
		// lastIdx := len(rf.logList) - 1
		// if args.LastLogTerm < rf.logList[lastIdx].Term || (args.LastLogTerm == rf.logList[lastIdx].Term && args.LastLogIndex < lastIdx) {
		// 	isValidCandidate = false
		// }
		lastRealIdx := rf.getLastRealIdx()
		if lastRealIdx == rf.lastIncludedIndex {
			if args.LastLogTerm < rf.lastIncludedTerm || (args.LastLogTerm == rf.lastIncludedTerm && args.LastLogIndex < rf.lastIncludedIndex) {
				isValidCandidate = false
			}
		} else {
			lastRfLog := rf.getLog(lastRealIdx)
			if args.LastLogTerm < lastRfLog.Term || (args.LastLogTerm == lastRfLog.Term && args.LastLogIndex < lastRealIdx) {
				isValidCandidate = false
			}
		}
	}
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		if isValidCandidate && rf.votedFor == -1 {
			rf.votedFor = args.CandidateID
			rf.persist()
			rf.found_leader_or_candidate = true
		}
		if rf.votedFor == args.CandidateID {
			DPrintf("ELECTION VOTE NEWS: candidate machine %v has received vote from machine %v for term %d!\n", args.CandidateID, rf.me, rf.currentTerm)
			reply.VoteGranted = true
		} else {
			DPrintf("ELECTION VOTE NEWS: candidate machine %v has FAILED to receive vote from machine %v for term %d!\n", args.CandidateID, rf.me, rf.currentTerm)
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
	reply.Term = args.Term
	if rf.votedFor == args.CandidateID {
		DPrintf("ELECTION VOTE NEWS: candidate machine %v has received vote from machine %v for term %d!\n", args.CandidateID, rf.me, rf.currentTerm)
		reply.VoteGranted = true
	} else {
		DPrintf("ELECTION VOTE NEWS: candidate machine %v has FAILED to receive vote from machine %v for term %d!\n", args.CandidateID, rf.me, rf.currentTerm)
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
	//index = len(rf.logList) //should we put -1 here???
	index = rf.getRealLogLength()
	term = rf.currentTerm
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		snapshotNeeded := false
		if rf.nextIndex[i] < rf.getRealLogLength() {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				//send snapshot instead for this round of advertisement
				snapshotNeeded = true
			} else {
				snapshotNeeded = false
			}
		} else {
			snapshotNeeded = false
		}
		if snapshotNeeded == false {
			var aeargs AppendEntryArgs
			aeargs.Term = rf.currentTerm
			aeargs.LeaderID = rf.me
			aeargs.PrevLogIndex = rf.nextIndex[i] - 1
			if rf.nextIndex[i] < rf.getRealLogLength() {
				// rf.nextIndex[i] > rf.lastIncludedIndex guaranteed
				aeargs.Entries = rf.logList[rf.getLogIdx(rf.nextIndex[i]):]
			} else {
				aeargs.Entries = nil
			}
			//keep filling in AppendEntryArgs
			if aeargs.PrevLogIndex >= 0 {
				if aeargs.PrevLogIndex <= rf.lastIncludedIndex {
					aeargs.PrevLogTerm = rf.lastIncludedTerm
				} else {
					aeargs.PrevLogTerm = rf.getLog(aeargs.PrevLogIndex).Term
				}
			}
			aeargs.LeaderCommit = rf.commitIndex
			//send append entries
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
							if rf.commitIndex < rf.matchIndex[peerID] && rf.getLog(rf.matchIndex[peerID]).Term == rf.currentTerm {
								//only current term can commit by counting
								//since it is guaranteed that matchIndex > commitIndex >= lastIncludedIndex(kv server state),
								//we don't need to worry about rf.matchIndex[peerID] overflow
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
									rf.applyCond.Signal()
								}
							}
						} else {
							//appendEntry fails
							if aereply.MatchIndex == -1 {
								//no log in PrevLogTerm
								if aeargs.PrevLogIndex <= rf.lastIncludedIndex {
									//need to send snapshot next time
									rf.nextIndex[peerID] = rf.lastIncludedIndex - 1 //this will guarantee to trigger leader sending snapshot?
								} else {
									//aeargs.PrevLogIndex > rf.lastIncludedIndex >= -1
									curPreIndex := aeargs.PrevLogIndex - 1
									for curPreIndex > rf.lastIncludedIndex && rf.getLog(curPreIndex).Term >= aeargs.PrevLogTerm {
										curPreIndex--
									}
									if curPreIndex <= rf.lastIncludedIndex {
										rf.nextIndex[peerID] = rf.lastIncludedIndex + 1 //last chance
									} else {
										rf.nextIndex[peerID] = curPreIndex + 1
									}
								}
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
		} else {
			//send snapshot instead
			DPrintf("Leader machine %d at term %d decides to send install snapshot request to follower machine %d instead\n", rf.me, rf.currentTerm, i)
			var isargs InstallSnapshotArgs
			isargs.Term = rf.currentTerm
			isargs.LeaderID = rf.me
			isargs.LastIncludedIndex = rf.lastIncludedIndex
			isargs.LastIncludedTerm = rf.lastIncludedTerm
			isargs.Data = rf.persister.ReadSnapshot()
			go func(peerID int, isargs InstallSnapshotArgs) {
				var isreply InstallSnapshotReply
				ok := rf.sendInstallSnapshot(peerID, &isargs, &isreply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != "leader" {
						return
					}
					if isreply.Term > rf.currentTerm {
						rf.currentTerm = isargs.Term
						rf.state = "follower"
						rf.votedFor = -1
						rf.persist()
						return
					}
					if isreply.Term == rf.currentTerm {
						if rf.matchIndex[peerID] < isargs.LastIncludedIndex {
							rf.matchIndex[peerID] = isargs.LastIncludedIndex
						}
						rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
					} else {
						//do nothing
					}

				}
			}(i, isargs)
		}
	}
	return index, term, isLeader
}

//
func (rf *Raft) StartIfSpaceAllow(command interface{}, maxRaftState int) (int, int, bool, bool) {
	index := -1
	term := -1
	isLeader := true
	isBusy := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		isLeader = false
		return index, term, isLeader, isBusy
	}
	if maxRaftState != -1 && rf.GetRaftStateSize() > maxRaftState*8/10 {
		isBusy = true
		return index, term, isLeader, isBusy
	}
	//leader appends the log and send out appendEntry request to followers
	rf.logList = append(rf.logList, LogEntry{command, rf.currentTerm})
	rf.persist()
	//index = len(rf.logList) //should we put -1 here???
	index = rf.getRealLogLength()
	term = rf.currentTerm
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		snapshotNeeded := false
		if rf.nextIndex[i] < rf.getRealLogLength() {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				//send snapshot instead for this round of advertisement
				snapshotNeeded = true
			} else {
				snapshotNeeded = false
			}
		} else {
			snapshotNeeded = false
		}
		if snapshotNeeded == false {
			var aeargs AppendEntryArgs
			aeargs.Term = rf.currentTerm
			aeargs.LeaderID = rf.me
			aeargs.PrevLogIndex = rf.nextIndex[i] - 1
			if rf.nextIndex[i] < rf.getRealLogLength() {
				// rf.nextIndex[i] > rf.lastIncludedIndex guaranteed
				aeargs.Entries = rf.logList[rf.getLogIdx(rf.nextIndex[i]):]
			} else {
				aeargs.Entries = nil
			}
			//keep filling in AppendEntryArgs
			if aeargs.PrevLogIndex >= 0 {
				if aeargs.PrevLogIndex <= rf.lastIncludedIndex {
					aeargs.PrevLogTerm = rf.lastIncludedTerm
				} else {
					aeargs.PrevLogTerm = rf.getLog(aeargs.PrevLogIndex).Term
				}
			}
			aeargs.LeaderCommit = rf.commitIndex
			//send append entries
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
							if rf.commitIndex < rf.matchIndex[peerID] && rf.getLog(rf.matchIndex[peerID]).Term == rf.currentTerm {
								//only current term can commit by counting
								//since it is guaranteed that matchIndex > commitIndex >= lastIncludedIndex(kv server state),
								//we don't need to worry about rf.matchIndex[peerID] overflow
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
									rf.applyCond.Signal()
								}
							}
						} else {
							//appendEntry fails
							if aereply.MatchIndex == -1 {
								//no log in PrevLogTerm
								if aeargs.PrevLogIndex <= rf.lastIncludedIndex {
									//need to send snapshot next time
									rf.nextIndex[peerID] = rf.lastIncludedIndex - 1 //this will guarantee to trigger leader sending snapshot?
								} else {
									//aeargs.PrevLogIndex > rf.lastIncludedIndex >= -1
									curPreIndex := aeargs.PrevLogIndex - 1
									for curPreIndex > rf.lastIncludedIndex && rf.getLog(curPreIndex).Term >= aeargs.PrevLogTerm {
										curPreIndex--
									}
									if curPreIndex <= rf.lastIncludedIndex {
										rf.nextIndex[peerID] = rf.lastIncludedIndex + 1 //last chance
									} else {
										rf.nextIndex[peerID] = curPreIndex + 1
									}
								}
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
		} else {
			//send snapshot instead
			DPrintf("Leader machine %d at term %d decides to send install snapshot request to follower machine %d instead\n", rf.me, rf.currentTerm, i)
			var isargs InstallSnapshotArgs
			isargs.Term = rf.currentTerm
			isargs.LeaderID = rf.me
			isargs.LastIncludedIndex = rf.lastIncludedIndex
			isargs.LastIncludedTerm = rf.lastIncludedTerm
			isargs.Data = rf.persister.ReadSnapshot()
			go func(peerID int, isargs InstallSnapshotArgs) {
				var isreply InstallSnapshotReply
				ok := rf.sendInstallSnapshot(peerID, &isargs, &isreply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != "leader" {
						return
					}
					if isreply.Term > rf.currentTerm {
						rf.currentTerm = isargs.Term
						rf.state = "follower"
						rf.votedFor = -1
						rf.persist()
						return
					}
					if isreply.Term == rf.currentTerm {
						if rf.matchIndex[peerID] < isargs.LastIncludedIndex {
							rf.matchIndex[peerID] = isargs.LastIncludedIndex
						}
						rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
					} else {
						//do nothing
					}

				}
			}(i, isargs)
		}
	}
	return index, term, isLeader, isBusy
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	//initialize for lab3B
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	go rf.handleTimeOut()
	go rf.applyLogsThread()

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
	rvargs := &RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: rf.getLastRealIdx()}
	// if len(rf.logList) > 0 {
	// 	rvargs.LastLogTerm = rf.logList[len(rf.logList)-1].Term
	// }
	if rf.getRealLogLength() > 0 {
		rvargs.LastLogTerm = rf.getLastTerm()
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
								//rf.nextIndex[peerID] = len(rf.logList)
								rf.nextIndex[peerID] = rf.getRealLogLength()
								rf.matchIndex[peerID] = -1
							}
							rf.logList = append(rf.logList, LogEntry{nil, rf.currentTerm})
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
			snapshotNeeded := false
			if rf.nextIndex[i] < rf.getRealLogLength() {
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					//send snapshot instead for this round of advertisement
					snapshotNeeded = true
				} else {
					snapshotNeeded = false
				}
			} else {
				snapshotNeeded = false
			}
			if snapshotNeeded == false {
				var aeargs AppendEntryArgs
				aeargs.Term = rf.currentTerm
				aeargs.LeaderID = rf.me
				aeargs.PrevLogIndex = rf.nextIndex[i] - 1
				if rf.nextIndex[i] < rf.getRealLogLength() {
					// rf.nextIndex[i] > rf.lastIncludedIndex guaranteed
					aeargs.Entries = rf.logList[rf.getLogIdx(rf.nextIndex[i]):]
				} else {
					aeargs.Entries = nil
				}
				//keep filling in AppendEntryArgs
				if aeargs.PrevLogIndex >= 0 {
					if aeargs.PrevLogIndex <= rf.lastIncludedIndex {
						aeargs.PrevLogTerm = rf.lastIncludedTerm
					} else {
						aeargs.PrevLogTerm = rf.getLog(aeargs.PrevLogIndex).Term
					}
				}
				aeargs.LeaderCommit = rf.commitIndex
				//send append entries
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
								DPrintf("APPENDENTRY request SUCCEED: to machine %d made by leader machine %v at term %d !\n", peerID, aeargs.LeaderID, rf.currentTerm)
								DPrintf("rf.nextIndex[peerID]: %d, returned matchIndex: %d,  sent log lengths: %d, current log length: %d, current commitIndex: %d, lastIncludedIndex: %d\n", rf.nextIndex[peerID], aereply.MatchIndex, len(aeargs.Entries), len(rf.logList), rf.commitIndex, rf.lastIncludedIndex)
								rf.nextIndex[peerID] = aereply.MatchIndex + 1
								rf.matchIndex[peerID] = aereply.MatchIndex
								if rf.commitIndex < rf.matchIndex[peerID] {
									DPrintf("rf.getLog(rf.matchIndex[peerID]).Term: %d, rf.currentTerm: %d\n", rf.getLog(rf.matchIndex[peerID]).Term, rf.currentTerm)
								}
								if rf.commitIndex < rf.matchIndex[peerID] && rf.getLog(rf.matchIndex[peerID]).Term == rf.currentTerm {
									//only current term can commit by counting
									//since it is guaranteed that matchIndex > commitIndex >= lastIncludedIndex(kv server state),
									//we don't need to worry about rf.matchIndex[peerID] overflow
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
										DPrintf("APPENDENTRY caused new logs to be committed: to machine %d made by leader machine %v at term %d !\n", peerID, aeargs.LeaderID, rf.currentTerm)
										rf.commitIndex = rf.matchIndex[peerID]
										rf.applyCond.Signal()
									} else {
										DPrintf("APPENDENTRY no log committed: to machine %d made by leader machine %v at term %d !\n", peerID, aeargs.LeaderID, rf.currentTerm)
									}
								}
							} else {
								//appendEntry fails
								if aereply.MatchIndex == -1 {
									//no log in PrevLogTerm
									if aeargs.PrevLogIndex <= rf.lastIncludedIndex {
										//need to send snapshot next time
										rf.nextIndex[peerID] = rf.lastIncludedIndex - 1 //this will guarantee to trigger leader sending snapshot?
									} else {
										//aeargs.PrevLogIndex > rf.lastIncludedIndex >= -1
										curPreIndex := aeargs.PrevLogIndex - 1
										for curPreIndex > rf.lastIncludedIndex && rf.getLog(curPreIndex).Term >= aeargs.PrevLogTerm {
											curPreIndex--
										}
										if curPreIndex <= rf.lastIncludedIndex {
											rf.nextIndex[peerID] = rf.lastIncludedIndex + 1 //last chance
										} else {
											rf.nextIndex[peerID] = curPreIndex + 1
										}
									}
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
			} else {
				//send snapshot instead
				DPrintf("Leader machine %d at term %d decides to send install snapshot request to follower machine %d instead\n", rf.me, rf.currentTerm, i)
				var isargs InstallSnapshotArgs
				isargs.Term = rf.currentTerm
				isargs.LeaderID = rf.me
				isargs.LastIncludedIndex = rf.lastIncludedIndex
				isargs.LastIncludedTerm = rf.lastIncludedTerm
				isargs.Data = rf.persister.ReadSnapshot()
				go func(peerID int, isargs InstallSnapshotArgs) {
					var isreply InstallSnapshotReply
					ok := rf.sendInstallSnapshot(peerID, &isargs, &isreply)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.state != "leader" {
							return
						}
						if isreply.Term > rf.currentTerm {
							rf.currentTerm = isargs.Term
							rf.state = "follower"
							rf.votedFor = -1
							rf.persist()
							return
						}
						if isreply.Term == rf.currentTerm {
							if rf.matchIndex[peerID] < isargs.LastIncludedIndex {
								rf.matchIndex[peerID] = isargs.LastIncludedIndex
							}
							rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
						} else {
							//do nothing
						}

					}
				}(i, isargs)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(magicNumber) * time.Millisecond)
	}
}

//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("INSTALLSNAPSHOT request received from machine %d for follower machine %d at term %d!\n", args.LeaderID, rf.me, rf.currentTerm)
	//step 1
	if args.Term < rf.currentTerm {
		DPrintf("INSTALLSNAPSHOT request from machine %d dropped for follower machine %d at term %d!\n", args.LeaderID, rf.me, rf.currentTerm)
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
	//ignore old request
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		DPrintf("INSTALLSNAPSHOT: old request from machine %d detected. Follower machine %d at term %d!\n", args.LeaderID, rf.me, rf.currentTerm)
		DPrintf("args.LastIncludedIndex: %d, rf.lastIncludedIndex: %d \n", args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}
	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("INSTALLSNAPSHOT snapshot info not useful:  request from machine %d detected. Follower machine %d at term %d!\n", args.LeaderID, rf.me, rf.currentTerm)
		DPrintf("args.LastIncludedIndex: %d, rf.lastIncludedIndex: %d \n", args.LastIncludedIndex, rf.lastIncludedIndex)
	}
	//backup: args.LastIncludedIndex + 1 < rf.getRealLogLength() && ((args.LastIncludedIndex == rf.lastIncludedIndex && args.LastIncludedTerm == rf.lastIncludedTerm) || rf.getLog(args.LastIncludedIndex).Term == args.LastIncludedTerm)

	if len(rf.logList) > 0 && rf.getLogIdx(args.LastIncludedIndex) >= 0 && rf.getLogIdx(args.LastIncludedIndex) < len(rf.logList) && rf.getLog(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		//if existing log entry has same index and term as snapshot's last included entry
		//retain log entries following it
		rf.logList = rf.logList[rf.getLogIdx(args.LastIncludedIndex+1):]
		//do we need to return here immediately, as described by the paper?
	} else {
		//discard the entire log
		rf.logList = make([]LogEntry, 0)
	}
	rf.persist()
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	rfStates := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(rfStates, args.Data)
	if rf.lastApplied > rf.lastIncludedIndex {
		return
	}
	rf.applyCh <- ApplyMsg{CommandIndex: rf.lastIncludedIndex, Command: args.Data, CommandValid: false}
	DPrintf("INSTALLSNAPSHOT request from machine %d succeed for follower machine %d at term %d!\n", args.LeaderID, rf.me, rf.currentTerm)
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
	cond1 := rf.getRealLogLength() <= args.PrevLogIndex
	cond2 := (args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm)
	//return rf.logList[realIdx-rf.lastIncludedIndex-1]
	cond3 := (rf.getLogIdx(args.PrevLogIndex) >= 0 && rf.getLogIdx(args.PrevLogIndex) < len(rf.logList) && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm)
	if args.PrevLogIndex >= 0 && (cond1 || cond2 || cond3) {
		//is it possible that args.PrevLogIndex < rf.lastIncludedIndex?
		//we have args.PrevLogIndex >= leader.lastIncludedIndex >= rf.lastIncludedIndex guaranteed,
		//therefore if rf.getLog(args.PrevLogIndex) overflows, then args.PrevLogIndex == rf.lastIncludedIndex
		DPrintf("APPENDENTRY: machine %d finds a prev log mismatch from leader machine %v at term %d !\n", rf.me, args.LeaderID, rf.currentTerm)
		DPrintf("PrevLogIndex: %d, Log Real Length: %d\n", args.PrevLogIndex, rf.getRealLogLength())
		if rf.getRealLogLength() > args.PrevLogIndex {
			if args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm {
				DPrintf("found args.PrevLogIndex == rf.lastIncludedIndex \n")
				DPrintf("arg.PrevLogTerm: %d, rf.lastIncludedTerm: %d \n", args.PrevLogTerm, rf.lastIncludedTerm)
			} else {
				pos := rf.getLogIdx(args.PrevLogIndex)
				if pos >= 0 && pos < len(rf.logList) {
					DPrintf("rf.logList[args.PrevLogIndex].Term: %d, args.Term: %d \n", rf.logList[pos].Term, args.Term)
				}
			}
		}
		//we can't find prevLog
		//heart beat message can reach here if:
		// 1) a follower crashed and missed some logs
		// 2) the follower becomes online again

		matchIndex := rf.getRealLogLength() - 1
		if matchIndex > args.PrevLogIndex {
			matchIndex = args.PrevLogIndex
		}
		// find the first index it stores for that term
		for matchIndex > rf.lastIncludedIndex {
			if rf.getLog(matchIndex).Term == args.PrevLogTerm {
				break
			}
			if rf.getLog(matchIndex).Term < args.PrevLogTerm {
				matchIndex = -1
				break
			}
			matchIndex--
		}
		// If rf.getRealLogLength() <= args.PrevLogIndex
		// 		either 1) matchIndex > rf.lastIncludeIndex -> we find the log with largest index not in snapshot that matches PrevLogTerm
		//     		or 2) matchIndex = -1 -> we cannot find the log not in snapshot that matches PrevLogTerm, install snapshot is a safe option
		//     		or 3) matchIndex == rf.lastIncludedIndex
		//				-> either matchIndex log matches PrevLogTerm, or we cannot find the log not in snapshot that matches PrevLogTerm
		//				-> install snapshot is a safe option, but can do better?
		// If args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm
		// 		we always have matchIndex = rf.lastIncludedIndex -> install snapshot is needed
		// If args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm
		// 		either 1) matchIndex > rf.lastIncludeIndex -> we find the log with largest index not in snapshot that matches PrevLogTerm
		//     		or 2) matchIndex = -1 -> we cannot find the log not in snapshot that matches PrevLogTerm, install snapshot is a safe option
		//     		or 3) matchIndex == rf.lastIncludedIndex
		//				-> either matchIndex log matches PrevLogTerm, or we cannot find the log not in snapshot that matches PrevLogTerm
		//				-> install snapshot is a safe option, but can do better?
		if matchIndex == rf.lastIncludedIndex {
			matchIndex = -1 //just let leader send the snapshot to us
		}
		reply.Success = false
		reply.MatchIndex = matchIndex
	} else if args.Entries != nil {
		DPrintf("APPENDENTRY: machine %d adding new logs from leader machine %v at term %d !\n", rf.me, args.LeaderID, rf.currentTerm)
		//we can find the prevLog
		//should not truncate the log !!!
		n := len(args.Entries)
		for i := 1; i <= n; i++ {
			//we are guaranteed that args.PrevLogIndex >= leader.lastIncludedIndex >= rf.lastIncludedIndex
			//therefore if rf.getLog(args.PrevLogIndex+i) overflows, then i=0 and args.PrevLogIndex == rf.lastIncludedIndex
			//but here i>0, so won't overflow
			cond1 := rf.getLogIdx(args.PrevLogIndex+i) >= 0
			cond2 := rf.getLogIdx(args.PrevLogIndex+i) < len(rf.logList)
			if args.PrevLogIndex+i < rf.getRealLogLength() && (cond1 && cond2 && rf.getLog(args.PrevLogIndex+i).Term != args.Entries[i-1].Term) {
				//existing entry conflicts with a new one
				rf.logList = rf.logList[:rf.getLogIdx(args.PrevLogIndex+i)] //truncate the log
				break                                                       //all following logs deleted, no need to loop anymore
			}
		}
		for i := 1; i <= n; i++ {
			cond1 := rf.getLogIdx(args.PrevLogIndex+i) >= 0
			cond2 := rf.getLogIdx(args.PrevLogIndex+i) < len(rf.logList)
			if args.PrevLogIndex+i < rf.getRealLogLength() && cond1 && cond2 {
				rf.logList[rf.getLogIdx(args.PrevLogIndex+i)] = args.Entries[i-1]
			} else {
				if args.PrevLogIndex+i >= rf.getRealLogLength() {
					rf.logList = append(rf.logList, args.Entries[i-1])
				}
			}
		}
		// rf.logList = rf.logList[:args.PrevLogIndex+1]
		// rf.logList = append(rf.logList, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if args.LeaderCommit >= args.PrevLogIndex+n {
				rf.commitIndex = args.PrevLogIndex + n
			}
			rf.applyCond.Signal()
		}
		reply.Success = true
		reply.MatchIndex = args.PrevLogIndex + n
		DPrintf("PrevLogIndex: %d, rf Log real length: %d\n", args.PrevLogIndex, rf.getRealLogLength())
	} else {
		DPrintf("APPENDENTRY: machine %d receives a heart beat msg from leader machine %v at term %d !\n", rf.me, args.LeaderID, rf.currentTerm)
		//heart beat
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if args.LeaderCommit >= args.PrevLogIndex {
				rf.commitIndex = args.PrevLogIndex
			}
			rf.applyCond.Signal()
		}
		reply.Success = true
		reply.MatchIndex = args.PrevLogIndex
		DPrintf("PrevLogIndex: %d, rf Log real Length: %d\n", args.PrevLogIndex, rf.getRealLogLength())
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
	if rf.commitIndex > rf.getRealLogLength()-1 {
		rf.commitIndex = rf.getRealLogLength() - 1
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		DPrintf("COMMITLOG: machine %d is going to commit log index %d: {command %v written term %v} at term %d\n", rf.me, i, rf.getLog(i).Command, rf.getLog(i).Term, rf.currentTerm)
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		//DPrintf("COMMITLOG: machine %d is going to commit log index %d: {command %v written term %v} at term %d\n", rf.me, i, rf.logList[i].Command, rf.logList[i].Term, rf.currentTerm)
		rf.applyCh <- ApplyMsg{CommandIndex: i + 1, Command: rf.getLog(i).Command, CommandValid: true}
	}
	//just make sure we are not going back, maybe not needed?
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) applyLogsThread() {
	for {
		rf.applyCond.L.Lock()
		rf.applyCond.Wait()
		rf.applyCond.L.Unlock()
		rf.mu.Lock()
		if rf.dead == 1 {
			//this machine is dead, no need to loop anymore
			rf.mu.Unlock()
			return
		}
		//sanity check
		if rf.commitIndex > rf.getRealLogLength()-1 {
			rf.commitIndex = rf.getRealLogLength() - 1
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			DPrintf("COMMITLOG: machine %d is going to commit log index %d: {command %v written term %v} at term %d\n", rf.me, i, rf.getLog(i).Command, rf.getLog(i).Term, rf.currentTerm)
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			//DPrintf("COMMITLOG: machine %d is going to commit log index %d: {command %v written term %v} at term %d\n", rf.me, i, rf.logList[i].Command, rf.logList[i].Term, rf.currentTerm)
			if rf.getLog(i).Command != nil {
				rf.applyCh <- ApplyMsg{CommandIndex: i + 1, Command: rf.getLog(i).Command, CommandValid: true}
			}
		}
		//just make sure we are not going back, maybe not needed?
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
	}
}

//
func (rf *Raft) GetRaftStateSize() int {
	ans := rf.persister.RaftStateSize()
	return ans
}

//
func (rf *Raft) TakeSnapshot(logIdx int, serverStates []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if logIdx <= rf.lastIncludedIndex || rf.lastIncludedIndex >= rf.getLastRealIdx() {
		//old snapshot request, ignore
		DPrintf("Old snapshot request for machine %d detected. logIdx: %d, rf.lastIncludedIndex: %d, rf.commitIndex %d, current log length: %d. Ignoring...\n", rf.me, logIdx, rf.lastIncludedIndex, rf.commitIndex, len(rf.logList))
		//DPrintf("logIdx: %d, rf.lastIncludedIndex: %d, rf.commitIndex %d, current log length: %d\n", logIdx, rf.lastIncludedIndex, rf.commitIndex, len(rf.logList))
		return
	}
	DPrintf("machine %d processing snapshot request at term %d. raft real log length: %d, lastIncludedIndex %d, passed-in logIdx: %d \n", rf.me, rf.currentTerm, rf.getRealLogLength(), rf.lastIncludedIndex, logIdx)
	//DPrintf("raft real log length: %d, lastIncludedIndex %d, passed-in logIdx: %d \n", rf.getRealLogLength(), rf.lastIncludedIndex, logIdx)

	relativelogIdx := rf.getLogIdx(logIdx)
	if relativelogIdx < 0 || relativelogIdx >= len(rf.logList) {
		DPrintf("FATAL failure incoming: relativelogIdx %d, len(rf.logList): %d \n", relativelogIdx, len(rf.logList))
	}
	rf.lastIncludedTerm = rf.getLog(logIdx).Term
	rf.lastIncludedIndex = logIdx

	rf.logList = rf.logList[relativelogIdx+1:]
	rf.persist()
	rfStates := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(rfStates, serverStates)
}
