package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    uint32
	Txn   uint32
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Id  uint32
	Txn uint32
	Key string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
