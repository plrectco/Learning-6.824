package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1
const clearqueuerate int = 100

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	GetOrPut bool
	Put      PutAppendArgs
	Index    uint32
	Server   int
}

type PendingCmd struct {
	args     interface{}
	done     chan bool
	logIndex int
	term     int
}

type TransactionStatus struct {
	txn     uint32
	success bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage map[string]string

	// Pending cmd that has been sent to Raft
	cmdid uint32
	queue map[uint32]*PendingCmd

	currentTerm  int
	currentIndex int

	txnSeen map[uint32]uint32
	txnDone map[uint32]TransactionStatus

	isAlive bool

	txnCond  *sync.Cond
	txnMutex sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if args != nil && reply != nil {

		// We don't care about the Get Txn
		// We need to get it anyway
		// It is by itself idempotent.
		// kv.txnMutex.Lock()
		// if kv.txnSeen[args.Id] < args.Txn {
		//     kv.txnSeen[args.Id] = args.Txn
		// }
		// kv.txnMutex.Unlock()

		i := atomic.AddUint32(&kv.cmdid, 1)
		kv.mu.Lock()
		index, term, isLeader := kv.rf.Start(Op{
			GetOrPut: false,
			Index:    i,
			Server:   kv.me,
		})
		if !isLeader {
			kv.mu.Unlock()
			reply.WrongLeader = true
		} else {
			// Not necessarily being the leader.
			pcmd := PendingCmd{
				args:     args,
				done:     make(chan bool),
				logIndex: index,
				term:     term,
			}
			kv.queue[i] = &pcmd
			kv.mu.Unlock()
			// Received the commit message.
			ok := <-pcmd.done
			DPrintf("get a commit for Get %s", args.Key)

			if ok {
				// kv.txnMutex.Lock()
				// if kv.txnDone[args.Id] < args.Txn {
				//     kv.txnDone[args.Id] = args.Txn
				// }
				// kv.txnMutex.Unlock()
				reply.WrongLeader = false
				kv.mu.Lock()
				v, found := kv.storage[args.Key]
				if !found {
					DPrintf("server %d committed %d key not found for %s", kv.me, index, args.Key)
					reply.Err = ErrNoKey
				} else {
					DPrintf("server %d committed %d key found for %s", kv.me, index, args.Key)
					reply.Err = OK
					reply.Value = v
				}
				kv.mu.Unlock()
			} else {
				reply.WrongLeader = true
			}
			DPrintf("%d finish committed index %d, ok %t", kv.me, index, ok)

		}

	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if args != nil && reply != nil {
		// May want to deal the case where Op is not valid.
		// If the current server has seen the txn
		// It means it has been processed by it and it is the leader at that time.

		// Can have multiple repeated request with the same txn.
		// Still need to lock to allow only one thread getting in.
		kv.txnMutex.Lock()
		DPrintf("server %d client %d put txn %d keyvalue %s:%s obtain lock", kv.me, args.Id, args.Txn, args.Key, args.Value)
		if clientTxnSeen, found := kv.txnSeen[args.Id]; found && clientTxnSeen > args.Txn {
			kv.txnMutex.Unlock()
			return
		}
		// For the same txn, should wait until done.
		if kv.txnSeen[args.Id] == args.Txn {
			// If it is the current transaction, wait until it is done.
			// the current txn must have set the txn and open the channel.
			// found is for the first transaction of the client.
			_, found := kv.txnDone[args.Id]
			if found {
				for kv.txnDone[args.Id].txn < args.Txn {
					kv.txnCond.Wait()
				}
				if kv.txnDone[args.Id].success {
					kv.txnMutex.Unlock()
					reply.Err = OK
					reply.WrongLeader = false
					return
				}
				// If not successful, try it again.
			}
			// If not found, proceed.
		}

		kv.txnSeen[args.Id] = args.Txn
		// let the request of the same txn return
		kv.mu.Lock()

		i := atomic.AddUint32(&kv.cmdid, 1)
		var argCopy PutAppendArgs
		argCopy = *args
		index, term, isLeader := kv.rf.Start(Op{
			GetOrPut: true,
			Put:      argCopy,
			Index:    i,
			Server:   kv.me,
		})
		if !isLeader {
			kv.mu.Unlock()
			reply.WrongLeader = true
			kv.txnDone[args.Id] = TransactionStatus{success: false, txn: args.Txn}

		} else {
			// Not necessarily being the leader.
			pcmd := PendingCmd{
				args:     args,
				done:     make(chan bool),
				logIndex: index,
				term:     term,
			}
			// Don't need lock here.
			kv.queue[i] = &pcmd
			kv.mu.Unlock()
			// Received the commit message.
			kv.txnMutex.Unlock()
			ok := <-pcmd.done
			if ok {
				reply.Err = OK
				reply.WrongLeader = false
				DPrintf("%d server prepare with committed index %d, after update client %d", kv.me, index, args.Id)
				kv.txnMutex.Lock()
				kv.txnDone[args.Id] = TransactionStatus{success: true, txn: args.Txn}
			} else {
				reply.WrongLeader = true
				kv.txnMutex.Lock()
				kv.txnDone[args.Id] = TransactionStatus{success: false, txn: args.Txn}
			}
			DPrintf("%d server finish with committed index %d, ok %t", kv.me, index, ok)
		}
		kv.txnCond.Broadcast()
		kv.txnMutex.Unlock()

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) clearQueue() {
	for kv.isAlive {
		kv.mu.Lock()
		var toDelete []uint32
		for k, v := range kv.queue {
			if v.term < kv.currentTerm || (v.term == kv.currentTerm && v.logIndex < kv.currentIndex) {
				// These logs has been abort, because commits are
				// in order.
				v.done <- false
				toDelete = append(toDelete, k)
			}
		}
		for _, k := range toDelete {
			delete(kv.queue, k)
		}
		kv.mu.Unlock()
		<-time.After(time.Duration(clearqueuerate) * time.Millisecond)
	}
}

func (kv *KVServer) updateStorage(args *PutAppendArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args.Op {
	case "Put":
		kv.storage[args.Key] = args.Value
		DPrintf("server %d put success, client %d, txn %d %s: %s", kv.me, args.Id, args.Txn, args.Key, args.Value)
	case "Append":
		kv.storage[args.Key] += args.Value
		DPrintf("server %d append success, client %d, txn %d %s: %s", kv.me, args.Id, args.Txn, args.Key, args.Value)
	}

}

func (kv *KVServer) commitCmd() {
	for kv.isAlive {
		msg := <-kv.applyCh
		if msg.CommandValid {
			logEntry, ok := msg.Command.(raft.LogEntry)
			if ok {
				// the commited message will always be the newest log received.

				kv.mu.Lock()
				kv.currentTerm = logEntry.Term
				kv.currentIndex = msg.CommandIndex
				op, opok := logEntry.Command.(Op)
				if opok {
					v, found := kv.queue[op.Index]
					kv.mu.Unlock()
					if found && op.Server == kv.me {
						// transfer execution to the waiting rpc handler.
						v.done <- true
						delete(kv.queue, op.Index)
					}

					// If get a PutAppend, update storageg
					if op.GetOrPut {

						arg := &op.Put

						// // It can also be the message that has been deleted.
						// // should not have the following happen. Add it here just in case.
						// if kv.txnDone[arg.Id].txn < arg.Txn || (kv.txnDone[arg.Id].txn == arg.Txn && !kv.txnDone[arg.Id].success) {
						kv.updateStorage(arg)
						// }
					}

				} else {
					kv.mu.Unlock()
				}

			}
		}

	}
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
	labgob.Register(PutAppendArgs{})
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.maxraftstate = maxraftstate
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.storage = make(map[string]string)
	kv.cmdid = 0
	kv.queue = make(map[uint32]*PendingCmd)

	kv.currentTerm = 0
	kv.currentIndex = 0

	kv.txnSeen = make(map[uint32]uint32)

	kv.isAlive = true

	kv.txnDone = make(map[uint32]TransactionStatus)
	kv.txnCond = sync.NewCond(&kv.txnMutex)

	go kv.commitCmd()
	go kv.clearQueue()

	// You may need initialization code here.

	return kv
}
