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
	//	"bytes"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func Max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// Raft state
const (
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3
)

// Raft init index and state
const (
    INIT_INDEX = 0
    INIT_TERM = 0
    INVALID_TERM = -1
    NO_LEADER = -1
)

// Raft log content
type Log struct {
    Command interface{} // command
    LogIndex int // log index
    Term int // log term
    Commited bool // entry is commited or not
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
    state int
    leader int

    // Store to disk every write option
    currentTerm int
    voteFor int

    // Timeout
    lastHeartBeat int64

    // apply chan
    applyCh chan ApplyMsg

    // Log Entry
    logs []Log
    commitedIndex int
    matchIndex []int
}

func (rf *Raft) GetCurrentTermNoLock() int {
    return rf.currentTerm
}

func (rf *Raft) GetCurrentTermWithLock() int {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.GetCurrentTermNoLock()
}

func (rf *Raft) LogLen() int {
    return len(rf.logs)
}

func (rf *Raft) LastLogIndex() int {
    return rf.LogLen() - 1
}

func (rf *Raft) LastLogTerm() int {
    return rf.logs[rf.LastLogIndex()].Term
}

//
// revert to a follower.
//
func (rf *Raft) RevertToFollowerWithLock(term, leader, voteFor int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.RevertToFollowerNoLock(term, leader, voteFor)
}

//
// revert to a follower.
//
func (rf *Raft) RevertToFollowerNoLock(term, leader, voteFor int) {
    rf.currentTerm = term
    rf.leader = leader
    rf.voteFor = voteFor
    rf.state = FOLLOWER
    return
}

func (rf * Raft) GetStateWithLock() int {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.GetStateNoLock()
}

func (rf * Raft) GetStateNoLock() int {
    return rf.state
}

func (rf *Raft) GetMajorityCount() int {
    return len(rf.peers) / 2 + 1
}

func (rf *Raft) GetServerCount() int {
    return len(rf.peers)
}

func (rf *Raft) GetTermAtIndex(index int) int {
    if index >= INIT_INDEX && index <= rf.LastLogIndex() {
        return rf.logs[index].Term
    }
    return INVALID_TERM
}

func (rf *Raft) GetRaftInfoWithLock() (int, int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.leader
}

func (rf *Raft) GetLastHeartBeatWithLock() int64 {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.lastHeartBeat
}

func (rf *Raft) ResetTimeOutNoLock() {
    t := GetNowMillSecond()
    rf.lastHeartBeat = t
}

func (rf *Raft) ResetTimeOutWithLock() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.ResetTimeOutNoLock()
}

func (rf *Raft) IsMatchEntryNoLock(indexTerm, index int) bool {
    if index >= INIT_INDEX && index <= rf.LastLogIndex() && rf.logs[index].Term == indexTerm {
        return true
    }
    return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
    isleader = rf.state == LEADER
    // DPrintf("INFO: GetState info server %d term %d leader %v", rf.me, term, isleader)
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// Raft base information.
//
type RfBaseInfo struct {
    CurrentTerm int
    Server int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Info RfBaseInfo
    LastLogIndex int
    LastLogTerm int
    CommitedIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Info RfBaseInfo
    VoteFor int
}

type SendHeartBeatReq struct {
    Info RfBaseInfo
}

type SendHeartBeatRsp struct {
    Info RfBaseInfo
}

type AppendEntriesReq struct {
    Info RfBaseInfo
    PrevLogIndex int
    PrevLogTerm int
    Entries []Log
    CommitedIndex int
}

type AppendEntriesRsp struct {
    Info RfBaseInfo
    CommitedIndex int
    Succ bool
}

type CommitEntryReq struct {
    Info RfBaseInfo
    LogIndex int
    LogTerm int
}

type CommitEntryRsp struct {
    Info RfBaseInfo
    Succ bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // DPrintf("INFO: RequestVote server %d from %d args %v\n", rf.me, args.Info.Server, *args)
    if args.Info.CurrentTerm > rf.currentTerm && args.CommitedIndex >= rf.commitedIndex && (args.LastLogTerm > rf.LastLogTerm() || (args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.LastLogIndex())) {

        // DPrintf("INFO: RequestVote server %d from %d args %v\n", rf.me, args.Info.Server, *args)
        rf.RevertToFollowerNoLock(args.Info.CurrentTerm, args.Info.Server, args.Info.Server)
        rf.ResetTimeOutNoLock()

    } else if (args.Info.CurrentTerm == rf.currentTerm) {

        if rf.voteFor == -1 && args.CommitedIndex >= rf.commitedIndex && (args.LastLogTerm > rf.LastLogTerm() || (args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.LastLogIndex())) {

            rf.revertToFollowerNoLock(args.Info.CurrentTerm, args.Info.Server, args.Info.Server)
            rf.ResetTimeOutNoLock()

        }

    } else {

        // ignore
    }

    reply.Info.CurrentTerm = rf.currentTerm
    reply.Info.Server = rf.me
    reply.VoteFor = rf.voteFor

    return
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rsp *AppendEntriesRsp) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if req.Info.CurrentTerm >= rf.currentTerm {
        rf.ResetTimeOutNoLock()
        rf.revertToFollowerNoLock(req.Info.CurrentTerm, req.Info.Server, req.Info.Server)
    }

    if req.Info.CurrentTerm >= rf.currentTerm && rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) {

        // DPrintln("INFO: AppendEntries", rf.me, *req, rf.logs);
        if len(req.Entries) == 0 {
            // heartbeat
        } else {
            if rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) {
                // append entries
                endIdx := req.PrevLogIndex
                if endIdx < rf.commitedIndex {
                    endIdx = rf.commitedIndex
                }
                rf.logs = rf.logs[ : endIdx + 1]
                for _, log := range(req.Entries) {
                    if log.LogIndex <= endIdx {
                        continue
                    }
                    newLog := Log {
                        Command: log.Command,
                        LogIndex: log.LogIndex,
                        Term: log.Term,
                        Commited: false,
                    }
                    rf.logs = append(rf.logs, newLog)
                }
            } else {
                // ignore
            }
        }

        if rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) && req.CommitedIndex > rf.commitedIndex {
            // update commited index
            for idx := rf.commitedIndex + 1; idx <= req.CommitedIndex && idx <= rf.LastLogIndex(); idx++ {
                rf.logs[idx].Commited = true
                msg := ApplyMsg {
                    CommandValid: true,
                    Command: rf.logs[idx].Command,
                    CommandIndex: rf.logs[idx].LogIndex,
                }
                // DPrintf("INFO: server %d agree %v", rf.me, msg)
                rf.applyCh <- msg
                rf.commitedIndex++
            }
        }

        rsp.Info.CurrentTerm = rf.currentTerm
        rsp.Info.Server = rf.me
        rsp.CommitedIndex = rf.commitedIndex
        rsp.Succ = true

    } else {

        // ignore request
        rsp.Info.CurrentTerm = rf.currentTerm
        rsp.Info.Server = rf.me
        rsp.CommitedIndex = rf.commitedIndex
        rsp.Succ = false

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
// sendAppendEntries
// 
func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, rsp *AppendEntriesRsp) bool {
    return rf.peers[server].Call("Raft.AppendEntries", req, rsp)
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

    rf.mu.Lock()
    isLeader = rf.state == LEADER
    term = rf.currentTerm
    endIndex := -1
    startConsensus := false

    if isLeader {

        // protect consensus procedure
        log := Log {
            Command: command,
            LogIndex: rf.LogLen(),
            Term: term,
            Commited: false,
        }
        index = log.LogIndex
        rf.logs = append(rf.logs, log)
        endIndex = rf.LastLogIndex()
        startConsensus = true
    }
    // DPrintf("INFO: RaftConsensusNoLock server %d logs %v cmd %v\n", rf.me, rf.logs, command)
    rf.mu.Unlock()

    if startConsensus {
        go rf.RaftConsensusNoLock(term, endIndex, term, false)
        // DPrintf("INFO: RaftConsensusNoLock Start ret %v server %d endIndex %d cmd %v logs %v\n", ok, rf.me, endIndex, command, rf.logs)
    }

	return index, term, isLeader
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
    if atomic.LoadInt32(&rf.dead) == 1 {
        return
    }
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// Make a election.
//
func (rf *Raft) MakeElection() {

    // get some info
    rf.mu.Lock()
    startElection := false
    isFollower := rf.state == FOLLOWER
    test := []int{}
    if isFollower {
        rf.voteFor = rf.me
        rf.state = CANDIDATE
        startElection = true
        test = append(test, rf.me)
    }
    currentTerm := rf.currentTerm + 1
    lastLogIndex := rf.LastLogIndex()
    lastLogTerm := rf.LastLogTerm()
    commitedIndex := rf.commitedIndex
    rf.mu.Unlock()

    // if not a follower
    if !startElection {
        return
    }

    // DPrintf("INFO: MakeElection server %d MakeElection term %d\n", rf.me, currentTerm);

    var mu sync.Mutex
    cond := sync.NewCond(&mu)
    finished := 1
    voteCount := 1

    for server, _ := range rf.peers {
        if server == rf.me {
            continue
        }

        go func(server int) {

            req := RequestVoteArgs{}
            rsp := RequestVoteReply{}
            req.Info.CurrentTerm = currentTerm
            req.Info.Server = rf.me
            req.LastLogIndex = lastLogIndex
            req.LastLogTerm = lastLogTerm
            req.CommitedIndex = commitedIndex

            ok := rf.sendRequestVote(server, &req, &rsp)

            rf.mu.Lock()
            cond.L.Lock()
            finished++
            if ok {
                // get vote from server
                if req.Info.CurrentTerm == currentTerm && rsp.VoteFor == rf.me {
                    test = append(test, rsp.Info.Server)
                    voteCount++
                }
            } else {
                // DPrintf("INFO: sendRequestVote error from %d to %d currentTerm %d", rf.me, server, currentTerm)
            }

            if voteCount >= rf.GetMajorityCount() || finished == rf.GetServerCount() {
                cond.Broadcast()
            }
            cond.L.Unlock()
            rf.mu.Unlock()

            return
        } (server)
    }

    cond.L.Lock()
    for voteCount < rf.GetMajorityCount() && finished < rf.GetServerCount() {
        cond.Wait()
    }
    cond.L.Unlock()

    rf.mu.Lock()
    cond.L.Lock() // protect voteCount
    if voteCount >= rf.GetMajorityCount() && currentTerm == rf.currentTerm + 1 {
        // become leader
        rf.leader = rf.me
        rf.state = LEADER
        rf.currentTerm = currentTerm

        // init matchIndex as rf.LastLogIndex
        for i := 0; i < len(rf.peers); i++ {
            rf.matchIndex[i] = rf.LastLogIndex()
        }

        // DPrintf("INFO: %d become leader term %d time %v voter %v logs %v\n", rf.me, rf.currentTerm, GetNowMillSecond(), test, rf.logs)

        rf.ResetTimeOutNoLock()
        go rf.appendEntryTicker()

    } else {

        // MakeElection fail
        // DPrintf("INFO: %d do not become leader term %d voter %v logs %v\n", rf.me, rf.currentTerm, test, rf.logs)
        rf.revertToFollowerNoLock(rf.currentTerm, rf.leader, rf.voteFor)

    }
    cond.L.Unlock()
    rf.mu.Unlock()

    return
}

//
// revert to a follower.
//
func (rf *Raft) revertToFollower(term, leader, voteFor int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.revertToFollowerNoLock(term, leader, voteFor)
}

//
// revert to a follower.
//
func (rf *Raft) revertToFollowerNoLock(term, leader, voteFor int) {
    if term >= rf.currentTerm {
        rf.currentTerm = term
        rf.leader = leader
        rf.voteFor = voteFor
    }
    rf.state = FOLLOWER
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using

        // randome sleep for [150ms, 300ms]
        timeout := 0
        for true {
            timeout = rand.Intn(300) + 200
            time.Sleep(time.Duration(timeout) * time.Millisecond)
            // DPrintf("INFO: ticker server %d lastHeartBeat %v time %v\n", rf.me, rf.GetLastHeartBeatWithLock(), GetNowMillSecond());
            if rf.GetLastHeartBeatWithLock() + int64(timeout + 100) < GetNowMillSecond() {
                break
            }
        }

        // don't start a election
        isFollower := rf.GetStateWithLock() == FOLLOWER
        if !isFollower {
            continue
        }

        // time out
        // revert raft start to follower and start a election
        // DPrintf("INFO: MakeElection start server %d lastHeartBeat %v time %v\n", rf.me, rf.GetLastHeartBeatWithLock(), GetNowMillSecond())
        rf.MakeElection()
	}
}

//
// raft procedure at index
//
func (rf *Raft) RaftConsensusNoLock(indexTerm int, endIndex int, currentTerm int, waitAll bool) bool {

    consensusRet := false
    startConsensus := false
    rf.mu.Lock()
    if rf.state == LEADER && rf.IsMatchEntryNoLock(indexTerm, endIndex) && currentTerm == rf.currentTerm {
        startConsensus = true
    }
    matchIndex := make([]int, rf.GetServerCount())
    copy(matchIndex, rf.matchIndex)
    rf.mu.Unlock()

    if !startConsensus {
        return consensusRet
    }

    // 1.appendEntry to other raft servers
    majorityCount := rf.GetMajorityCount()
    var appendMu sync.Mutex
    appendCond := sync.NewCond(&appendMu)
    appendSuccCount := 1
    appendFinished := 1
    appendSuccArr := []int{rf.me}

    wg := sync.WaitGroup{}
    for server, _ := range(rf.peers) {
        if server == rf.me {
            continue
        }

        if waitAll {
            wg.Add(1)
        }
        go func(server int) {

            doAppend := true
            tmpPrevIndex := matchIndex[server]
            for tmpPrevIndex >= INIT_INDEX && doAppend {

                req := AppendEntriesReq{}
                rsp := AppendEntriesRsp{}

                rf.mu.Lock()
                req.Info.Server = rf.me
                req.Info.CurrentTerm = currentTerm
                req.PrevLogIndex = tmpPrevIndex
                req.PrevLogTerm = rf.GetTermAtIndex(tmpPrevIndex)
                req.CommitedIndex = rf.commitedIndex
                entries := []Log{}
                if rf.IsMatchEntryNoLock(indexTerm, endIndex) && tmpPrevIndex + 1 <= rf.LastLogIndex() {
                    entries = rf.logs[tmpPrevIndex + 1 : ]
                }
                req.Entries = entries
                rf.mu.Unlock()

                // rpc
                ok := rf.sendAppendEntries(server, &req, &rsp)

                rf.mu.Lock()
                appendCond.L.Lock()
                if ok && rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) {
                    if rsp.Info.CurrentTerm == currentTerm && rsp.Succ {
                        rf.matchIndex[server] = endIndex
                        appendSuccCount++
                        appendSuccArr = append(appendSuccArr, rsp.Info.Server)
                        doAppend = false
                    }
                }
                appendCond.L.Unlock()
                rf.mu.Unlock()

                tmpPrevIndex--
            }

            appendCond.L.Lock()
            appendFinished++
            if appendSuccCount >= majorityCount || appendFinished == rf.GetServerCount() {
                appendCond.Broadcast()
            }
            appendCond.L.Unlock()

            if waitAll {
                wg.Done()
            }
            return
        } (server)
    }

    appendCond.L.Lock()
    for appendSuccCount < majorityCount && appendFinished < rf.GetServerCount() {
        appendCond.Wait()
    }
    appendCond.L.Unlock()

    if waitAll {
        wg.Wait()
    }

    rf.mu.Lock()
    appendCond.L.Lock()
    if appendSuccCount >= majorityCount && rf.IsMatchEntryNoLock(indexTerm, endIndex) && rf.currentTerm == currentTerm {
        if indexTerm == currentTerm && rf.commitedIndex < endIndex {
            // commited
            for idx := rf.commitedIndex + 1; idx <= endIndex; idx++ {
                msg := ApplyMsg {
                    CommandValid: true,
                    Command: rf.logs[idx].Command,
                    CommandIndex: rf.logs[idx].LogIndex,
                }

                rf.logs[idx].Commited = true
                // DPrintf("INFO: server %d agree %v", rf.me, msg)
                rf.applyCh <- msg
            }
            rf.commitedIndex = endIndex
        }
        consensusRet = true
    }
    appendCond.L.Unlock()
    rf.mu.Unlock()

    return consensusRet
}

//
// append entries ticker
//
func (rf *Raft) appendEntryTicker() {
    for rf.killed() == false {

        time.Sleep(time.Duration(300) * time.Millisecond)

        rf.mu.Lock()
        indexTerm, endIndex := rf.LastLogTerm(), rf.LastLogIndex()
        currentTerm := rf.currentTerm
        rf.mu.Unlock()

        if rf.GetStateWithLock() == LEADER {
            go rf.RaftConsensusNoLock(indexTerm, endIndex, currentTerm, true)
            // DPrintf("INFO: RaftConsensusNoLock appendEntryTicke end ret %v server %d\n", ok, rf.me);
        } else {
            // not a leader yet
            break
        }
    }
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
    rf.state = FOLLOWER // init as a follower
    rf.leader = NO_LEADER  // now learder this time
    rf.lastHeartBeat = GetNowMillSecond()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    // load currentTerm and voteFor from disk
    rf.currentTerm = INIT_TERM
    rf.voteFor = -1

    rf.applyCh = applyCh

    // init logs
    rf.logs = make([]Log, 0)
    rf.logs = append(rf.logs, Log{Command: nil, LogIndex: INIT_INDEX, Term: INIT_TERM, Commited: true})
    rf.commitedIndex = 0
    rf.matchIndex = make([]int, len(rf.peers))
    for i := 0; i < len(rf.matchIndex); i++ {
        rf.matchIndex[i] = rf.LastLogIndex()
    }

    rand.Seed(time.Now().UnixNano())

    // DPrintln("INFO: Start a raft node")

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
