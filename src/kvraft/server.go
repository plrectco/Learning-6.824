package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1
const clearqueuerate int = 100
const requesttimeout int = 2000
const statecheckrate int = 50

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// To export.
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	OpId      uint32
	ClientId  int64
	TxNum     uint32
	ServerId  int
	Put       PutAppendArgs
}

type PendingOp struct {
	done  chan bool
	logId int
	term  int
}

// type TransactionStatus struct {
//     txn     uint32
//     success bool
// }

type KVServerPersistence struct {
	Storage map[string]string
	TxnDone map[int64]uint32
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
	opid         uint32
	pendingQueue map[uint32]*PendingOp

	currentIndex int

	// Store completed transaction number for each client
	txnDone map[int64]uint32
	isAlive bool

	persister *raft.Persister

	// txnCond  *sync.Cond
	// txnMutex sync.Mutex
}

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		s := "Server " + strconv.Itoa(kv.me) + ": "
		log.Printf(s+format, a...)
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if args != nil && reply != nil {
		if !kv.isAlive {
			reply.WrongLeader = true
			return
		}
		i := atomic.AddUint32(&kv.opid, 1)

		index, term, isLeader := kv.rf.Start(Op{
			Operation: "Get",
			OpId:      i,
			TxNum:     args.TxNum,
			ServerId:  kv.me,
			ClientId:  args.ClientId,
		})

		kv.DPrintf("Request to get for key %s, index %d, client %d, txn %d", args.Key, index, args.ClientId, args.TxNum)
		if !isLeader {
			reply.WrongLeader = true
		} else {
			// At this point, it is not necessarily the leader.
			pop := PendingOp{
				done:  make(chan bool),
				logId: index,
				term:  term,
			}
			kv.mu.Lock()
			kv.pendingQueue[i] = &pop
			kv.mu.Unlock()

			// Received the commit message.
			var ok bool
			select {
			case ok = <-pop.done:
			case <-time.After(time.Duration(requesttimeout) * time.Millisecond):
				ok = false
			}
			kv.mu.Lock()
			delete(kv.pendingQueue, i)
			kv.mu.Unlock()
			kv.DPrintf("Get a commit for key %s, index %d, client %d", args.Key, index, args.ClientId)

			if ok {
				reply.WrongLeader = false
				kv.mu.Lock()
				v, found := kv.storage[args.Key]
				if !found {
					kv.DPrintf("Committed %d key not found for %s", index, args.Key)
					reply.Err = ErrNoKey
				} else {
					kv.DPrintf("Committed %d key found for %s", index, args.Key)
					reply.Err = OK
					reply.Value = v
				}
				kv.mu.Unlock()
			} else {
				reply.WrongLeader = true
			}
			kv.DPrintf("finish committed index %d ok %t", index, ok)

		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if args != nil && reply != nil {
		if !kv.isAlive {
			reply.WrongLeader = true
			return
		}
		i := atomic.AddUint32(&kv.opid, 1)
		var argCopy PutAppendArgs
		argCopy = *args
		index, term, isLeader := kv.rf.Start(Op{
			Operation: "Put",
			OpId:      i,
			ServerId:  kv.me,
			TxNum:     args.TxNum,
			ClientId:  args.ClientId,
			Put:       argCopy,
		})
		kv.DPrintf("Request to put for key %s value %s, index %d, client %d, txn %d", args.Key, args.Value, index, args.ClientId, args.TxNum)

		if !isLeader {
			reply.WrongLeader = true
		} else {
			// Not necessarily being the leader.
			pop := PendingOp{
				done:  make(chan bool),
				logId: index,
				term:  term,
			}
			kv.mu.Lock()
			kv.pendingQueue[i] = &pop
			kv.mu.Unlock()
			var ok bool
			select {
			case ok = <-pop.done:
			case <-time.After(time.Duration(requesttimeout) * time.Millisecond):
				ok = false
			}
			kv.mu.Lock()
			delete(kv.pendingQueue, i)
			kv.mu.Unlock()
			if ok {
				reply.Err = OK
				reply.WrongLeader = false
				kv.DPrintf("prepare with committed index %d, after update client %d", index, args.ClientId)
			} else {
				reply.WrongLeader = true
			}
			kv.DPrintf("finish with committed index %d, value %s", index, args.Value)
		}
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
	// should stop the server after killing it.
	kv.isAlive = false
}

//
// This function clear all the pending ops that are expired
// so that the server can notify the client.
//
// func (kv *KVServer) clearQueue() {
//     for kv.isAlive {
//         kv.mu.Lock()
//         var toDelete []uint32
//         for k, v := range kv.pendingQueue {
//             if v.term < kv.currentTerm || (v.term == kv.currentTerm && v.logId < kv.currentIndex) {
//                 // These logs have been abort, because commits are
//                 // in order.
//                 v.done <- false
//                 toDelete = append(toDelete, k)
//             }
//         }
//         for _, k := range toDelete {
//             delete(kv.pendingQueue, k)
//         }
//         kv.mu.Unlock()
//         <-time.After(time.Duration(clearqueuerate) * time.Millisecond)
//     }
// }

//
// Check whether the server has seen this transaction
// It will also update the newest transaction this server has seen.
//
func (kv *KVServer) checkDuplicate(clientId int64, txn uint32) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	prevTxn, ok := kv.txnDone[clientId]
	if ok && prevTxn >= txn {
		kv.DPrintf("Duplicate found for client %d txn %d", clientId, txn)
		return true
	}
	kv.DPrintf("No duplicate detected: client %d prev %d current %d", clientId, prevTxn, txn)
	kv.txnDone[clientId] = txn
	return false

}

func (kv *KVServer) updateStorage(args *PutAppendArgs, clientId int64) {
	// It is either the first txn for the client or a newer transaction than seen.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args.Op {
	case "Put":
		kv.storage[args.Key] = args.Value
		DPrintf("server %d put success, %s:%s client %d, txn %d", kv.me, args.Key, args.Value, args.ClientId, args.TxNum)
	case "Append":
		kv.storage[args.Key] += args.Value
		DPrintf("server %d append success, %s:%s client %d, txn %d", kv.me, args.Key, args.Value, args.ClientId, args.TxNum)
	}

}

func (kv *KVServer) commitCmd() {
	for kv.isAlive {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if kv.currentIndex >= msg.CommandIndex {
			kv.mu.Unlock()
			continue
		}
		kv.currentIndex = msg.CommandIndex
		kv.mu.Unlock()
		if msg.CommandValid {

			// handling the case when the server is still catch up.

			// if kv.currentIndex != msg.CommandIndex-1 {
			//     kv.DPrintf("log index mismatch current %d committed %d", kv.currentIndex, msg.CommandIndex)
			//     continue
			// }

			// the commited message will always be the newest log received.
			op, opok := msg.Command.(Op)
			if opok {
				kv.DPrintf("Processing index %d client %d txn %d", msg.CommandIndex, op.ClientId, op.TxNum)
				// Apply to storage before return to client.
				isDuplicate := kv.checkDuplicate(op.ClientId, op.TxNum)
				if !isDuplicate && op.Operation == "Put" {
					kv.DPrintf("Put with committed index %d, client %d, txn %d", msg.CommandIndex, op.ClientId, op.TxNum)
					kv.updateStorage(&op.Put, op.ClientId)
				}

				kv.mu.Lock()
				v, found := kv.pendingQueue[op.OpId]
				kv.mu.Unlock()
				if found && op.ServerId == kv.me {
					// transfer execution to the waiting rpc handler.
					// The rpc can then reply to the client.
					select {
					case v.done <- true:
					case <-time.After(time.Millisecond):
					}
				}

			}
		} else {
			// snapshot.
			kv.DPrintf("Get a snapshot with index %d", msg.CommandIndex)
			kv.mu.Lock()
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			persistence := KVServerPersistence{}
			if err := d.Decode(&persistence); err != nil {
				DPrintf("snapshot decode fails %v size %d", err, len(msg.Snapshot))
			} else {
				kv.storage = persistence.Storage
				kv.txnDone = persistence.TxnDone
				kv.DPrintf("snapshot loaded %d", msg.CommandIndex)
			}

			kv.mu.Unlock()

		}
	}
}

func (kv *KVServer) snapshot() {
	for kv.isAlive {
		kv.mu.Lock()
		stateSize := kv.persister.RaftStateSize()
		if kv.currentIndex > 0 && kv.maxraftstate > 0 && stateSize > kv.maxraftstate {
			compactIndex := kv.currentIndex
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(KVServerPersistence{
				Storage: kv.storage,
				TxnDone: kv.txnDone,
			})
			data := w.Bytes()
			kv.DPrintf("Encode snapshot for %d size %d", compactIndex, len(data))
			kv.rf.CompactLog(compactIndex, data)
		}
		kv.mu.Unlock()
		<-time.After(time.Duration(statecheckrate) * time.Millisecond)

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
	kv.persister = persister
	kv.currentIndex = 0
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.storage = make(map[string]string)
	kv.opid = 0
	kv.pendingQueue = make(map[uint32]*PendingOp)

	kv.currentIndex = 0

	kv.txnDone = make(map[int64]uint32)

	kv.isAlive = true

	go kv.commitCmd()
	go kv.snapshot()
	// go kv.clearQueue()

	return kv
}
