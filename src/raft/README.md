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

A special design is the use of go channel.

For leader election, I want the main thread to block until either it receives more than half of the yes vote or it receives all the votes. This is like a 'or' logic between two conditions. 

Of course I can use condition variable, but after playing with it I find them to be way more slower than normal waitgroup(which is like a semaphore). Then it is the go channel comes to my mind: I can let the go channel accept a value when either of the 'or' logic is satisfied, and the main thread will pend on the value.

Since we only need the main thread to know about it once, I use a `hasNotified` flag to tell the children thread no need to send more values to avoid them sending value without other threads to receive. Then we need to implement a timeout on the channel we are listening to, in case either side of the channel is disconnected. 



