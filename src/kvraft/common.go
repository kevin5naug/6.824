package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicate   = "ErrDuplicate"
	ErrOpMismatch  = "ErrOpMismatch"
	ErrTimeOut     = "ErrTimeOut"
	ErrUnknown       = "ErrUnknown"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID int
	ClientID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpID int
	ClientID int64
}

type GetReply struct {
	Err   Err
	Value string
}
