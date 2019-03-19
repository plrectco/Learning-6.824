## Many many design decisions
### Idempotent, avoid duplication
Assumption: when one transaction has not finished, there will be no other transaction that has higher transaction number that it.

It follows that there can be multiple outstanding transactions that has lower or the same transaction number pending for reply.

Note that the for the Get method we should let it proceed no matter what: there is no point to return a previous result of a Get request even though we know we have read it before. And Get method by itself is idempotent and we do not need to do anything with it.

Deduplication options:

 Option 1 (Wrong)
 A message pool recording the transaction that this kvserver has processed as a leader.

**what if a txn is resent to another server who is the leader this term but do not record this transaction?**
=>  everyone should record the txn if it is commited.

Option 2
Dedup before sending to raft.

Race condition
There are multiple requests of the same txn coming in.
Should not let it proceed because the txn may be currently served by Raft and has not been committed yet. On the other hand, the current processed transaction may abort and we need to let the current awaiting request proceed.

DeDuplicate  should not let the request return without having the result commited. 

Do we want to give the failed txn another chance?
- if success txn, we don't let it go through raft again
- if failed txn, we need to save it.

Then I design the two following structures:
txnSeen[clientId] -> transaction that has been seen by the current server.
txnDone[clientId] -> transaction that is known to be finished.

(The following discussion apply only to Put calls)

Policy:
    - Return OK  to any transaction that has been finished.
    - For any transaction that has the same TxNum as txnSeen[clientId]
        - If the current transaction is done: return OK
        - If it failed, let the current request proceed.
        - If it is not done, wait.

Therefore we need a condition variable to hold the requests that are waiting for the current request and wake them when the current request finishes.


Why not just use either txnSeen or txnDone
To make sure the current transaction that is pending inside the raft server is not processed multiple times. If just use done, we can't know if this transaction is either being processed, or hasn't being put into the queue yet.


Option 3 (Final choice)
Accept duplicate log, but check it before applying to the storage.
Just use one txnSeen[client]Txn to compare.
If the commit has a lower transaction number then we just not apply it.

Since the commit flow is always in order (as guaranteed by raft log), a lower txNum means the server has applied it to the storage before.

Compared to Option 2
Cons:
    - it will contain duplicate log in Raft. Need to deduplicate when replaying the log.
Pros:
    - easy to implement
    - It revolutionalize the architecture (instread of client - kvstore - raft, it becomes client - raft - kvstore, which is more close to the intende behaviour)

### PendingOp
When the incoming message reach the kvserver, they will not be immediately committed and applied to the storage. On the other hand, we will let it go through the raft to make sure each peer know about this op before we decide to commit it to the storage. Therefore, we need to maintain a pending message pool so that we can reply to the orignal client when we are notified that the request they submit has been approved by raft. 

Problem is that, it may or may not be accepted. Therefore, we need to discard those pending op that has failed in raft side.

Option 1
Time out.

Option 2
Clear the pool when the currentTerm and logIndex has passed the term and log index of the pending op.


Comparison
Time out:
    - Time out doesn't need any information from Raft.
    - Time out need to choose a proper constant. If it is not big enough, it is possible that the request is actually committed in Raft but return the client a false reply.
    - Time out is more efficient in terms of both space and time

Clear Queue:
    - Queue clearing may have to wait for the election to finish, which increase the time.
    - A deadlock can exist, Raft is waiting for new commit mesage so that it can commit the logs from the previous leader, but client is waiting for the commit and can't put a new request.

If use clear queue method, we need
Term and logindex of commit
There will be no out of order log, but maintaining the current term and log can help us get rid of the pending command that has been already discarded by raft.


The deadlock situation actually quite common. When a new leader replaces an old leader, it need new commit message so that it can commit the message from the previous leader. 

On the other hand, client side implmentation can let us use time out on server side without causing any inconsistent trouble. If a put request is submitted to the raft but the request has been time out, the client will retry to the put request again to the server with the same transaction number. Sicne we have deduplication on the server side,  committing the old expired request is OK as it has the same effect as the new request and only one of them will be applied. The only difference is that there can be another client concurrently change the state in between the old and the new duplicate request, which will leads to a different state, but in the client's point of view it is still linearizable

### Misc
The Put method should be replicated on all servers. Even where there is no outstanding request.

Go Copy an interface is troublesome

The copy function copy nothing? I directly use the slice of the log and it works, compared to the nonworking copy version.

Should not let the commit message process when not having put the pending message into queue
Should save the previous 

It can be not the leader the first time and then become the leader for the second request.
Should only reply to the transaction that has seen as a leader.

cmdid is dependent on the server number, should not be used to determine whether a pending message can be found in the server or not.

Should turn off the Get and PutAppend RPC when the server is shutdown
This bug takes me two days to pinpoint. When you see a mismatch in the Get result and the expected result, it can be that the really old Get request is delayed till now. A more obvious syndrome is that an out of order commit index is present.

Go slice assignment does not copy, but array assignment does copy.

Server only takes snapshot when the commitindex is greater than 0, making sure that it does not overwrite the previous snapshot when it restarts. Note that if there is an existing snapshot it will be appended to the server state before accepting any other commit.

