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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// Raft state
const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

// Raft init index and state
const (
	INIT_INDEX   = 0
	INIT_TERM    = 0
	INVALID_TERM = -1
	NO_LEADER    = -1
)

// Raft log content
type Log struct {
	Command  interface{} // command
	LogIndex int         // log index
	Term     int         // log term
	Commited bool        // entry is commited or not
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state  int
	leader int

	// Store to disk every write option
	currentTerm int
	voteFor     int

	// Timeout
	lastHeartBeat int64

	// apply chan
	applyCh chan ApplyMsg
	innerCh chan ApplyMsg

	// Log Entry
	logs          []Log
	commitedIndex int
	matchIndex    []int
}

// get length of log
func (rf *Raft) LogLen() int {
	return len(rf.logs)
}

// get last log index of raft
func (rf *Raft) LastLogIndex() int {
	return rf.LogLen() - 1
}

// get last log term of raft
func (rf *Raft) LastLogTerm() int {
	return rf.logs[rf.LastLogIndex()].Term
}

// revert to a follower
func (rf *Raft) RevertToFollowerWithLock(term, leader, voteFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.RevertToFollowerNoLock(term, leader, voteFor)
	return
}

// revert to a follower without lock and persist
func (rf *Raft) RevertToFollowerNoLock(term, leader, voteFor int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
	}
	rf.leader = leader
	rf.voteFor = voteFor
	rf.state = FOLLOWER
	rf.persist()
	return
}

func (rf *Raft) SetCurrentTermNoLock(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.persist()
	}
	return
}

func (rf *Raft) SetVoteForNoLock(server int) {
	rf.voteFor = server
	rf.persist()
	return
}

func (rf *Raft) SetCommitedIndexNoLock(index int) {
	if index > rf.commitedIndex {
		rf.commitedIndex = index
	}
	rf.persist()
	return
}

func (rf *Raft) GetStateWithLock() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.GetStateNoLock()
}

func (rf *Raft) GetStateNoLock() int {
	return rf.state
}

func (rf *Raft) GetMajorityCount() int {
	return len(rf.peers)/2 + 1
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

func (rf *Raft) GetLastHeartBeatWithLock() int64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartBeat
}

func (rf *Raft) ResetTimeOutNoLock() {
	t := GetNowMillSecond()
	if t > rf.lastHeartBeat {
		rf.lastHeartBeat = t
	}
	return
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
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
	var voteFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {

		log.Fatal("ERR: readPersist error")

	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
	}
	return
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// Raft base information.
type RfBaseInfo struct {
	CurrentTerm int
	Server      int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Info          RfBaseInfo
	LastLogIndex  int
	LastLogTerm   int
	CommitedIndex int
	Time          int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Info    RfBaseInfo
	VoteFor int
}

type AppendEntriesReq struct {
	Info          RfBaseInfo
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []Log
	CommitedIndex int
	HeartBeat     bool
	Time          int64
}

type AppendEntriesRsp struct {
	Info          RfBaseInfo
	CommitedIndex int
	Succ          bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (args.Info.CurrentTerm > rf.currentTerm || (args.Info.CurrentTerm == rf.currentTerm && rf.voteFor == NO_LEADER)) && (args.LastLogTerm > rf.LastLogTerm() || (args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.LastLogIndex())) {

		// vote it
		rf.RevertToFollowerNoLock(args.Info.CurrentTerm, NO_LEADER, args.Info.Server)
		rf.ResetTimeOutNoLock()

	} else if args.Info.CurrentTerm > rf.currentTerm {

		rf.RevertToFollowerNoLock(args.Info.CurrentTerm, NO_LEADER, NO_LEADER)

	}

	reply.Info.CurrentTerm = rf.currentTerm
	reply.Info.Server = rf.me
	reply.VoteFor = rf.voteFor

	DPrintf("INFO: RequestVote info end rf.me %d req %v rsp %v time %v\n", rf.me, *args, *reply, GetNowMillSecond())

	return
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rsp *AppendEntriesRsp) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Info.CurrentTerm >= rf.currentTerm {
		rf.ResetTimeOutNoLock()
		rf.RevertToFollowerNoLock(req.Info.CurrentTerm, req.Info.Server, req.Info.Server)
	}
	rsp.Succ = false

	if req.Info.CurrentTerm >= rf.currentTerm && rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) {

		if rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) && rf.LastLogIndex() >= req.PrevLogIndex {
			// append entries
			endIdx := req.PrevLogIndex
			if endIdx < rf.commitedIndex {
				endIdx = rf.commitedIndex
			}

			if len(req.Entries) > 0 && !(rf.IsMatchEntryNoLock(req.Entries[len(req.Entries)-1].Term, req.Entries[len(req.Entries)-1].LogIndex)) {
				rf.logs = rf.logs[:endIdx+1]
				for _, log := range req.Entries {
					if log.LogIndex <= endIdx {
						continue
					}
					newLog := Log{
						Command:  log.Command,
						LogIndex: log.LogIndex,
						Term:     log.Term,
						Commited: false,
					}
					rf.logs = append(rf.logs, newLog)
					rf.persist()
				}
			}

			if req.CommitedIndex >= rf.commitedIndex {
				// update commited index
				for idx := rf.commitedIndex + 1; idx <= req.CommitedIndex && idx <= req.PrevLogIndex; idx++ {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[idx].Command,
						CommandIndex: rf.logs[idx].LogIndex,
					}
					rf.applyCh <- msg
					DPrintf("INFO: follower agree raft %d term %d msg %v\n", rf.me, rf.currentTerm, msg)
					rf.logs[idx].Commited = true
					rf.SetCommitedIndexNoLock(rf.commitedIndex + 1)
				}
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

	}

	DPrintf("INFO: AppendEntries end raft %d term %d req %v rsp %v time %v", rf.me, rf.currentTerm, *req, *rsp, GetNowMillSecond())

	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries
func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, rsp *AppendEntriesRsp) bool {
	return rf.peers[server].Call("Raft.AppendEntries", req, rsp)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	isLeader = rf.state == LEADER
	term = rf.currentTerm
	endIndex := -1
	commitedIndex := rf.commitedIndex
	startConsensus := false

	if isLeader && !rf.killed() {
		// protect consensus procedure
		log := Log{
			Command:  command,
			LogIndex: rf.LogLen(),
			Term:     term,
			Commited: false,
		}
		index = log.LogIndex
		rf.logs = append(rf.logs, log)
		endIndex = rf.LastLogIndex()
		startConsensus = true
		rf.persist()
	}
	rf.mu.Unlock()

	DPrintf("INFO: Start Consensus server %d term %v index %d endIndex %d cmd %v isleader %v\n", rf.me, term, index, endIndex, command, isLeader)
	if startConsensus {
		go rf.Consensus(term, endIndex, term, commitedIndex, false)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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

// Make a election.
func (rf *Raft) MakeElection(lastTerm int) {

	// get some info
	rf.mu.Lock()
	startElection := false
	isFollower := rf.state == FOLLOWER
	test := []int{}
	if isFollower && rf.currentTerm == lastTerm && !rf.killed() {
		rf.leader = rf.me
		rf.state = CANDIDATE
		startElection = true
		test = append(test, rf.me)
		rf.SetCurrentTermNoLock(rf.currentTerm + 1)
		rf.SetVoteForNoLock(rf.me)
	}
	currentTerm := rf.currentTerm
	lastLogIndex := rf.LastLogIndex()
	lastLogTerm := rf.LastLogTerm()
	commitedIndex := rf.commitedIndex
	time := GetNowMillSecond()
	rf.mu.Unlock()

	// if not a follower
	if !startElection {
		return
	}

	DPrintf("INFO: MakeElection server %d MakeElection term %d time %v\n", rf.me, currentTerm, time)

	cond := sync.NewCond(&rf.mu)
	finished := 1
	voteCount := 1
	voteFail := false
	startTime := GetNowMillSecond()
	rpcFailCount := 0

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
			req.Time = time
			startVote := false
			cond.L.Lock()
			if rf.currentTerm == currentTerm && rf.state == CANDIDATE && voteCount < rf.GetMajorityCount() {
				startVote = true
			}
			cond.L.Unlock()

			if !startVote {
				cond.Broadcast()
				return
			}

			ok := rf.sendRequestVote(server, &req, &rsp)

			cond.L.Lock()
			finished++
			if ok {
				// get vote from server
				if rsp.Info.CurrentTerm > currentTerm {
					voteFail = true
				} else if rsp.Info.CurrentTerm == currentTerm && rsp.VoteFor == rf.me {
					test = append(test, rsp.Info.Server)
					voteCount++
				}
			} else {
				rpcFailCount++
			}

			cond.L.Unlock()

			cond.Broadcast()

			return
		}(server)
	}

	cond.L.Lock()
	for !voteFail && voteCount < rf.GetMajorityCount() && finished < rf.GetServerCount() && rpcFailCount < rf.GetMajorityCount() {
		if GetNowMillSecond()-startTime > 300 {
			// timeout
			// voteFail = true
		}
		cond.Wait()
	}
	cond.L.Unlock()

	cond.L.Lock() // protect voteCount
	if !voteFail && voteCount >= rf.GetMajorityCount() && currentTerm == rf.currentTerm && rf.IsMatchEntryNoLock(lastLogTerm, lastLogIndex) {
		// become leader
		rf.leader = rf.me
		rf.state = LEADER

		// init matchIndex as rf.LastLogIndex
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i] = rf.LastLogIndex()
		}
		rf.logs = rf.logs[:lastLogIndex+1]
		rf.persist()

		DPrintf("INFO: %d become leader term %d time %v voter %v\n", rf.me, rf.currentTerm, GetNowMillSecond(), test)

		go rf.appendEntryTicker()

	} else {

		// MakeElection fail
		DPrintf("INFO: %d do not become leader election term %d term %d voter %v time %v\n", rf.me, currentTerm, rf.currentTerm, test, GetNowMillSecond())
		rf.RevertToFollowerNoLock(rf.currentTerm, NO_LEADER, rf.voteFor)
		rf.ResetTimeOutNoLock()

	}
	rf.persist()
	cond.L.Unlock()

	return
}

// AppendEntry at index
func (rf *Raft) Consensus(consensusTerm int, consensusIndex int, currentTerm int, commitedIndex int, waitAll bool) bool {
	consensusRet := false
	start := false
	rf.mu.Lock()
	if rf.state == LEADER && rf.IsMatchEntryNoLock(consensusTerm, consensusIndex) && currentTerm == rf.currentTerm && !rf.killed() {
		start = true
	}
	matchIndex := make([]int, rf.GetServerCount())
	copy(matchIndex, rf.matchIndex)
	rf.mu.Unlock()

	if !start {
		return consensusRet
	}

	consensusCond := sync.NewCond(&rf.mu)
	consensusSuccCount := 1
	consensusFinishCount := 1
	consensusSuccArray := []int{rf.me}
	consensusFail := false
	consensusStartTime := GetNowMillSecond()
	consensusWaitGroup := sync.WaitGroup{}
	time := GetNowMillSecond()

	DPrintf("INFO: Consensus start raft %d index %d term %d time %v\n", rf.me, consensusIndex, consensusTerm, time)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		if waitAll {
			consensusWaitGroup.Add(1)
		}

		go func(server int) {
			doConsens := true
			prevIndex := matchIndex[server]
			if prevIndex >= consensusIndex {
				prevIndex = consensusIndex
			}
			tryCount := 0

			for doConsens && prevIndex >= INIT_INDEX {
				req := AppendEntriesReq{}
				rsp := AppendEntriesRsp{}

				consensusCond.L.Lock()
				startConsensus := false
				req.Info.Server = rf.me
				req.Info.CurrentTerm = currentTerm
				req.PrevLogIndex = prevIndex
				req.PrevLogTerm = rf.GetTermAtIndex(prevIndex)
				req.CommitedIndex = commitedIndex
				req.Entries = []Log{}
				req.Time = time
				if rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) && rf.IsMatchEntryNoLock(consensusTerm, consensusIndex) && currentTerm == rf.currentTerm {
					start := req.PrevLogIndex + 1
					end := consensusIndex
					if start <= end {
						req.Entries = rf.logs[start : end+1]
					}
					startConsensus = true
				}
				if prevIndex == rf.LastLogIndex() {
					req.HeartBeat = true
				}
				consensusCond.L.Unlock()

				if !startConsensus {
					doConsens = false
					continue
				}

				ok := rf.sendAppendEntries(server, &req, &rsp)

				consensusCond.L.Lock()
				if ok {
					if rsp.Info.CurrentTerm > currentTerm {
						doConsens = false
						consensusFail = true
					} else if rsp.Info.CurrentTerm == currentTerm {
						if rsp.Succ == true && rf.IsMatchEntryNoLock(req.PrevLogTerm, req.PrevLogIndex) {
							consensusSuccCount++
							consensusSuccArray = append(consensusSuccArray, rsp.Info.Server)
							doConsens = false
							rf.matchIndex[server] = consensusIndex
						}
					}
				} else {
					doConsens = false
				}
				tryCount++
				prevIndex--
				if rsp.Info.CurrentTerm == currentTerm && !rsp.Succ && tryCount > 3 {
					if rsp.CommitedIndex < prevIndex {
						prevIndex = rsp.CommitedIndex
					}
					tryCount = 0
				}
				consensusCond.L.Unlock()
			}

			consensusCond.L.Lock()
			consensusFinishCount++
			consensusCond.L.Unlock()

			consensusCond.Broadcast()

			if waitAll {
				consensusWaitGroup.Done()
			}
			return
		}(server)
	}

	consensusCond.L.Lock()
	for !consensusFail && consensusSuccCount < rf.GetMajorityCount() && consensusFinishCount < rf.GetServerCount() {
		if GetNowMillSecond()-consensusStartTime > 300 {
			// consensusFail = true
		}
		consensusCond.Wait()
	}
	consensusCond.L.Unlock()

	if waitAll {
		consensusWaitGroup.Wait()
	}

	consensusCond.L.Lock()
	if !consensusFail && consensusSuccCount >= rf.GetMajorityCount() && rf.IsMatchEntryNoLock(consensusTerm, consensusIndex) && currentTerm == rf.currentTerm {
		// if consensusTerm == currentTerm && rf.commitedIndex < consensusIndex {
		if rf.commitedIndex < consensusIndex {
			for idx := rf.commitedIndex + 1; idx <= consensusIndex && idx <= rf.LastLogIndex(); idx++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[idx].Command,
					CommandIndex: rf.logs[idx].LogIndex,
				}
				rf.applyCh <- msg
				DPrintf("INFO: leader agree raft %d term %d msg %v array %v\n", rf.me, currentTerm, msg, consensusSuccArray)
				rf.logs[idx].Commited = true
				rf.SetCommitedIndexNoLock(rf.commitedIndex + 1)
			}
			consensusRet = true
		}
	}
	consensusCond.L.Unlock()

	return consensusRet
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using

		timeout := 0
		for true {
			timeout = rand.Intn(200) + 300
			timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
			<-timer.C

			rf.mu.Lock()
			nowTime := GetNowMillSecond()
			lastHeartBeat := rf.lastHeartBeat
			rf.mu.Unlock()
			if lastHeartBeat+int64(timeout) < nowTime {
				break
			}
		}

		// don't start a election
		rf.mu.Lock()
		isFollower := rf.state == FOLLOWER
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		if !isFollower || rf.killed() {
			continue
		}

		// time out
		// revert raft start to follower and start a election
		go rf.MakeElection(currentTerm)
	}
}

// append entries ticker
func (rf *Raft) appendEntryTicker() {
	for rf.killed() == false {

		rf.mu.Lock()
		indexTerm, endIndex := rf.LastLogTerm(), rf.LastLogIndex()
		currentTerm := rf.currentTerm
		commitedIndex := rf.commitedIndex
		isLeader := rf.state == LEADER
		rf.mu.Unlock()

		if isLeader {
			go rf.Consensus(indexTerm, endIndex, currentTerm, commitedIndex, false)
		} else {
			// not a leader yet
			break
		}

		rf.mu.Lock()
		rf.ResetTimeOutNoLock()
		rf.mu.Unlock()

		timer := time.NewTimer(time.Duration(150) * time.Millisecond)
		<-timer.C
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.mu.Lock()

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER   // init as a follower
	rf.leader = NO_LEADER // now learder this time
	rf.lastHeartBeat = GetNowMillSecond()

	// load currentTerm and voteFor from disk
	rf.currentTerm = INIT_TERM
	rf.voteFor = -1

	rf.applyCh = applyCh
	rf.innerCh = make(chan ApplyMsg, 1000)

	// init logs
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Command: nil, LogIndex: INIT_INDEX, Term: INIT_TERM, Commited: true})

	rf.commitedIndex = 0
	rand.Seed(time.Now().UnixNano())

	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for _, log := range rf.logs {
		if log.Commited {
			rf.commitedIndex = log.LogIndex
		}
	}

	DPrintln("INFO: Start a raft node", rf.me, rf)

	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf

}
