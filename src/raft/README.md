# Raft

## Structure
`Make` will create a Raft instance to call. `Start` is use to send client request to the Raft instance. 

Inside `Make`, we initialize the Raft instance, kick off *leader election* go routine and an `apply message` go routine. 

- `Make`
    - `leaderElection`
        - `sendRequestVote`
        - `boostrapLeader`
            - `heartBeat`
                -`AppendEntries`
    - `applyLogToServer`



## Design
**MultiThread**
A special design is the use of go channel.

For leader election, I want the main thread to block until either it receives more than half of the yes vote or it receives all the votes. This is like a 'or' logic between two conditions. 

Of course I can use condition variable, but after playing with it I find them to be way more slower than normal waitgroup(which is like a semaphore). Then it is the go channel comes to my mind: I can let the go channel accept a value when either of the 'or' logic is satisfied, and the main thread will pend on the value.

Since we only need the main thread to know about it once, I use a `hasNotified` flag to tell the children thread no need to send more values to avoid them sending value without other threads to receive. Then we need to implement a timeout on the channel we are listening to, in case either side of the channel is disconnected. 

Another issue of multi threading is that log length of the leader may change when it is sending heartbeats. Thus the log length may be different when the leader initiate an appendEntry RPC and when it receives the reply. We need to save the temporary log length for reference at the time it received the reply. Failing to do so will cause the matchindex to be larger than the actual value, and overconfident in committing entries.

**VotedFor**
This field is to make sure no node can vote more than once for one specific term. However, that means when a RPC request shows that there is already a higher term, the callee should increase its term, convert to a follower and reset VotedFor.


**AppendEntryReply**
Per the possible improvement from the paper, I change the appendEntryReply structure to include a field indicating the nextIndex for the leader to append. Without such a field a leader will have to decrement the entry one by one which will cause test2CUnreliable to fail. Adding such field improves much efficiency of the system.

## Issues
Availability Issue.
Because of intense lock use, the avalability may be an issue. There could be some requests being delayed for long.

