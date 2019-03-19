package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	mrand "math/rand"
)

var clientIndex uint32 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int    // leader index
	txn    uint32 // transaction number, to achieve idempotent
	id     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.leader = mrand.Int() % len(servers)
	ck.servers = servers
	ck.txn = 0
	// ck.id = atomic.AddUint32(&clientIndex, 1)
	ck.id = nrand()
	// You'll have to add code here.
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
	ck.txn++
	arg := GetArgs{
		Key:      key,
		ClientId: ck.id,
		TxNum:    ck.txn,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &arg, &reply)
		if (ok && reply.WrongLeader) || !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			DPrintf("From leader %d: get %s : %s", ck.leader, arg.Key, reply.Value)
			return reply.Value
		}

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
	ck.txn++
	arg := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		TxNum:    ck.txn,
	}
	for {
		// Must re-init a new reply for each try
		var reply PutAppendReply
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &arg, &reply)

		if (ok && reply.WrongLeader) || !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else {
			return
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
