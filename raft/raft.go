package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Data interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"zls/labrpc"
)

// ApplyMsg is the successive log Entries, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Data         []byte
}

func (msg *ApplyMsg) String() string {
	return fmt.Sprintf("[Valid:%v;Data:%v;Index:%v]",
		msg.CommandValid, msg.Command, msg.CommandIndex)
}

type StateType string

// three states of a Raft server
const (
	FOLLOWER  StateType = "FOLLOWER"
	CANDIDATE           = "CANDIDATE"
	LEADER              = "LEADER"
)

const CheckDuration = 10
const MinElectionDuration = 400
const MaxElectionDuration = 800
const HeartbeatDuration = 100

// Raft implements a single Raft peer
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	state             StateType
	receiveVoteCount  int
	leaderId          int
	electionTimeout   time.Duration // the election timeout
	lastElectionTime  time.Time     // the lastElectionTime time at which the peer heard from the leader
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	applyCh           chan ApplyMsg
	applyCond         sync.Cond
	heartbeatCond     sync.Cond
	lastIncludedIndex int // for raft snapshot
	lastIncludedTerm  int

	// logs for running raft
	T, D, I, W, E *log.Logger
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;Leader:%d;LogLen:%d;CommitIndex:%d;LastApplied:%d;LastIncludedIndex:%d]",
		rf.state, rf.me, rf.currentTerm, rf.votedFor, rf.leaderId, len(rf.log), rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("[Term:%d;CandidateId:%d;LastLogIndex:%d;LastLogTerm:%d]",
		args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // the current Term
	VoteGranted bool // true means candidate received votes
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("[Term:%d;VoteGranted:%v]", reply.Term, reply.VoteGranted)
}

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("[Term:%d;LeaderId:%d;PrevLogIndex:%d;PrevLogTerm:%d;EntryLen:%d;LeaderCommit:%d]",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
}

// AppendEntriesReply indicates RPC reply result
type AppendEntriesReply struct {
	Term    int
	Success bool
	// for log inconsistency (optimize)
	XTerm  int
	XIndex int
	XLen   int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("[Term:%d;Success:%v]", reply.Term, reply.Success)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // send all data
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[Term:%d;LeaderId:%d;LastIncludeIndex:%d;LastIncludeTerm:%d]",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

//
// change a raft peer to candidate state
//
func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.receiveVoteCount = 1
	rf.leaderId = -1
	rf.persist()
	rf.D.Printf("%s become candidate", rf)
}

// change a raft node become follower
func (rf *Raft) becomeFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.receiveVoteCount = 0
	rf.leaderId = -1
	rf.persist()
	rf.D.Printf("%s become follower", rf)
}

//
// convert a raft peer to leader
//
func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	rf.leaderId = rf.me
	rf.votedFor = -1
	rf.receiveVoteCount = 0
	rf.persist()
	// initialize nextIndex and matchIndex when raft peer become leader
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
			rf.matchIndex[i] = rf.nextIndex[i] - 1
		} else {
			rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
			rf.matchIndex[i] = 0
		}
	}
	rf.heartbeatCond.Signal()
	rf.D.Printf("%s become leader", rf)
}

//
// the election loop periodically check to see whether the time since then is greater than the timeout period
// use time.Sleep() with a small constant argument to drive the periodic checks
//
func (rf *Raft) electionLoop() {
	for {
		// the raft instance is killed, quit loop
		rf.mu.Lock()
		// the instance is killed, stop the loop
		if rf.killed() {
			rf.becomeFollower(0)
			rf.mu.Unlock()
			return
		}
		now := time.Now()
		// the leader ignores the election timer
		if now.Sub(rf.lastElectionTime) < rf.electionTimeout {
			rf.mu.Unlock()
			// sleep if there is no timeout happen
			time.Sleep(CheckDuration * time.Millisecond)
		} else {
			rf.D.Printf("%s election timer timeout", rf)
			if rf.isLeader() {
				rf.resetTimer()
				rf.mu.Unlock()
			} else {
				// election timeout, the raft peer becomes a candidate and starts election
				rf.mu.Unlock()
				// start election process
				rf.startElection()
			}
		}
	}
}

//
// the leader runs heartbeat loop to prevent election
//
func (rf *Raft) heartbeatLoop() {
	for {
		rf.mu.Lock()
		for !rf.killed() && !rf.isLeader() {
			rf.heartbeatCond.Wait()
		}

		// the raft peer is killed, stop the loop
		if rf.killed() {
			rf.becomeFollower(0)
			rf.mu.Unlock()
			return
		}

		// send rpc requests to other peers
		for i := 0; i < len(rf.peers); i++ {
			rf.sendEntry(i)
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatDuration * time.Millisecond)
	}
}

// apply log to state machine
func (rf *Raft) applyLoop() {
	for {
		rf.mu.Lock()
		for !rf.killed() && rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}
		// this raft peer is killed
		if rf.killed() {
			rf.becomeFollower(0)
			rf.mu.Unlock()
			return
		}
		// send till the end
		for rf.lastApplied < rf.commitIndex {
			// send snapshot to replicate state machine
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.D.Printf("%s send snapshot at index %d", rf, rf.lastApplied)
				rf.lastApplied = rf.lastIncludedIndex
				entry := ApplyMsg{
					CommandValid: false,
					Data:         rf.persister.ReadSnapshot(),
				}
				rf.applyCh <- entry
			} else {
				rf.D.Printf("%s send command at index %d", rf, rf.lastApplied)
				rf.lastApplied++
				entry := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Data,
					CommandIndex: rf.lastApplied,
				}
				rf.applyCh <- entry
			}
		}
		rf.mu.Unlock()
	}
}

// when election timout elipeces, start election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.becomeCandidate()
	// reset timer
	rf.resetTimer()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1 + rf.lastIncludedIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			isCandidate := rf.state == CANDIDATE
			rf.mu.Unlock()
			if isCandidate {
				var reply RequestVoteReply
				// each RPC should probably be sent (and its reply processed) in its own goroutine
				// there are two reasons
				// unreachable peers don't delay the collection of a majority of replies
				// the heartbeat and election timers can continue to tick at all times
				go rf.issueRequestVote(i, &args, &reply)
			} else {
				// this peer is not a candidate, quit
				return
			}
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader()
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) resetTimer() {
	rf.lastElectionTime = time.Now()
	// Cautions: Need to choose a random timeout
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Int63n(MaxElectionDuration-MinElectionDuration) + MinElectionDuration
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.currentTerm", rf)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.votedFor", rf)
	}
	// ignore dummy head
	err = e.Encode(rf.log)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.log", rf)
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.lastIncludedIndex", rf)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.D.Printf("%s persist [Term:%d;VotedFor:%d;LogLen:%d]", rf, rf.currentTerm, rf.votedFor, len(rf.log))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.D.Printf("%s cannot read from persister", rf)
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm, votedFor, lastIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil {
		rf.D.Printf("%v cannot decode rf.currentTerm", rf)
	}
	if d.Decode(&votedFor) != nil {
		rf.D.Printf("%v cannot encode rf.votedFor", rf)
	}
	if d.Decode(&log) != nil {
		rf.D.Printf("%v cannot encode rf.log", rf)
	}
	if d.Decode(&lastIndex) != nil {
		rf.D.Printf("%v cannot encode rf.lastIncludedIndex", rf)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIndex
	rf.commitIndex = lastIndex
	rf.D.Printf("%s read persist [Term:%d;VotedFor:%d;LogLen:%d]", rf, rf.currentTerm, rf.votedFor, len(rf.log))
}

// save snapshot and corresponding raft state
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.currentTerm", rf)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.votedFor", rf)
	}
	// ignore dummy head
	err = e.Encode(rf.log)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.currentTerm", rf)
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		rf.D.Printf("%v cannot encode rf.lastIncludedIndex", rf)
	}
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.D.Printf("%s receive RequestVote rpc request %s", rf, args)

	// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if Term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.D.Printf("%v reject RequestVote rpc from %v for (term)", rf, args)
		return
	}

	// votedFor is not null and not candidateId
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.D.Printf("%s has voted for raft peer %d", rf, rf.votedFor)
		return
	}

	// "up-to-date" check
	if !rf.upToDateCheck(args) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.D.Printf("%v reject RequestVote rpc from %v for (out-of-date log)", rf, args)
		return
	}

	// grant vote for leader
	rf.votedFor = args.CandidateID
	rf.persist()
	rf.resetTimer()

	reply.Term = args.Term
	reply.VoteGranted = true

	rf.D.Printf("%s grants vote for raft peer %d", rf, rf.votedFor)
}

//
// the candidate's log MUST at least up-to-date to receiver's log
//
func (rf *Raft) upToDateCheck(args *RequestVoteArgs) bool {
	lastIndex := len(rf.log) - 1 + rf.lastIncludedIndex
	lastTerm := rf.log[lastIndex-rf.lastIncludedIndex].Term
	if lastTerm != args.LastLogTerm {
		return args.LastLogTerm > lastTerm
	}
	return args.LastLogIndex >= lastIndex
}

//
// issue each request vote in a goroutine
//
func (rf *Raft) issueRequestVote(peerId int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(peerId, args, reply)
	if !ok {
		// rf.D.Printf("RequestVote %v cannot send to raft peer %d", args, peerId)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.D.Printf("%v receives RequestVote reply %v from raft peer %d", rf, reply, peerId)

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		// reset timer
		//rf.resetTimer()
	}

	// check the term and the state of current raft peer
	if args.Term != rf.currentTerm || rf.state != CANDIDATE {
		rf.D.Printf("%v is not candidate or changes from previous term: %v", rf, args.Term)
		return
	}

	// process RequestVote reply
	if reply.VoteGranted {
		rf.receiveVoteCount++
		if rf.receiveVoteCount >= len(rf.peers)/2+1 {
			rf.D.Printf("%s receive vote from majority of servers", rf)
			// become a leader
			rf.becomeLeader()
		}
	}
}

//
// AppendEntries RPC call
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.D.Printf("%s receive AppendEntries request %s", rf, args)

	// If RPC request or response contains Term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.D.Printf("%v reject AppendEntries rpc %v for (term)", rf, args)
		return
	}

	// change to be follower
	if rf.state != FOLLOWER {
		rf.becomeFollower(args.Term)
	}
	// reset timer
	rf.resetTimer()
	rf.leaderId = args.LeaderId

	// maybe the rpc is delayed
	prevLogTerm := -1
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < rf.lastIncludedIndex+len(rf.log) {
		prevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log)+rf.lastIncludedIndex <= args.PrevLogIndex || prevLogTerm != args.PrevLogTerm {
		// return false because of log inconsistency
		reply.Term = rf.currentTerm
		reply.Success = false
		// add parameters to roll back quickly
		// XTerm:  term in the conflicting entry (if any)
		// XIndex: index of first entry with that term (if any)
		// XLen:   log length
		reply.XLen = len(rf.log) + rf.lastIncludedIndex
		if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.XTerm = prevLogTerm
			reply.XIndex = rf.lastIncludedIndex + 1
		} else {
			if reply.XLen > args.PrevLogIndex {
				reply.XTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
				reply.XIndex = args.PrevLogIndex
				for reply.XIndex > rf.lastIncludedIndex && rf.log[reply.XIndex-1-rf.lastIncludedIndex].Term == reply.XTerm {
					reply.XIndex--
				}
			}
		}

		rf.D.Printf("%v reject AppendEntries rpc %v for (log inconsistency)", rf, args)
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	nextIndex := args.PrevLogIndex + 1 - rf.lastIncludedIndex
	var i int
	for i = 0; i < len(args.Entries) && nextIndex < len(rf.log); i, nextIndex = i+1, nextIndex+1 {
		// the log at the same index has different term, delete logs following it
		if rf.log[nextIndex].Term != args.Entries[i].Term {
			rf.log = rf.log[0:nextIndex]
			rf.D.Printf("%s log before %d is consist", rf, nextIndex)
			break
		}
	}

	// Append any new entries not already in the log
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}
	rf.persist()
	rf.D.Printf("%v matches leader logs", rf)

	// set the reply to true
	reply.Term = rf.currentTerm
	reply.Success = true
	// update commitIndex
	// TODO: explain why?
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = MinInt(args.LeaderCommit, len(rf.log)-1+rf.lastIncludedIndex)
		rf.D.Printf("%s new commit index is %d", rf, rf.commitIndex)
		// wakeup apply loop
		rf.applyCond.Signal()
		//rf.applyLog()
	}
}

func (rf *Raft) issueAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendHeartBeats(peerId, args, reply)
	if !ok {
		// retry
		// rf.D.Printf("AppendEntries %v cannot send to raft peer %d", args, peerId)
		return
	}

	// first check the current term and the args term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.D.Printf("%s receive AppendEntries reply %s", rf, reply)

	// first check whether the term has changed
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}

	// first check the state and the term hasn't change
	if args.Term != rf.currentTerm || rf.state != LEADER {
		rf.D.Printf("%v is not leader or changes from previous term: %v", rf, args.Term)
		return
	}

	// process reply result
	if reply.Success {
		// the send handler success
		// update matchIndex and nextIndex
		rf.matchIndex[peerId] = MaxInt(rf.matchIndex[peerId], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
		rf.D.Printf("%s update raft peer %d: [matchIndex:%d;nextIndex:%d]",
			rf, peerId, rf.matchIndex[peerId], rf.nextIndex[peerId])
		// check index N for updating commitIndex
		// reference: https://zhuanlan.zhihu.com/p/103849249 (THANKS!)
		majority := rf.findMajoritySameIndex(rf.matchIndex)
		if majority > rf.commitIndex && rf.log[majority-rf.lastIncludedIndex].Term == rf.currentTerm {
			rf.commitIndex = majority
			rf.D.Printf("%v advance commit index to %v", rf, rf.commitIndex)
			rf.applyCond.Signal()
			//rf.applyLog()
		}
	} else {
		// request false due to log inconsistency

		// optimize: to roll back quickly
		// based on mit 6.824 class note (https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt)

		// follower's log is too short: nextIndex = XLen
		if reply.XLen <= args.PrevLogIndex {
			rf.nextIndex[peerId] = reply.XLen
		} else {
			if reply.XTerm == -1 {
				rf.nextIndex[peerId] = reply.XIndex
			} else {
				// find whether leader's log contains reply.XTerm
				index := args.PrevLogIndex - rf.lastIncludedIndex
				if index < 0 {
					rf.nextIndex[peerId] = rf.lastIncludedIndex + 1
				} else {
					for index > 0 && rf.log[index].Term > reply.XTerm {
						index--
					}
					// leader has XTerm: nextIndex = leader's last entry for XTerm
					if rf.log[index].Term == reply.XTerm {
						rf.nextIndex[peerId] = index + rf.lastIncludedIndex
					} else {
						// leader doesn't have XTerm: nextIndex = XIndex
						rf.nextIndex[peerId] = reply.XIndex
					}
				}
			}
		}
	}
}

// find the majority next index of all peers
// TODO: use quick selection algorithm
func (rf *Raft) findMajoritySameIndex(matchIndex []int) int {
	temp := make([]int, len(matchIndex))
	copy(temp, matchIndex)
	// down sort
	sort.Sort(sort.Reverse(sort.IntSlice(temp)))
	// find the middle one
	index := len(rf.peers) / 2
	return temp[index]
}


// InstallSnapshot handles snapshot request
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.D.Printf("%s receive InstallSnapshot request %s", rf, args)
	// If RPC request or response contains Term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	// ignore request if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// need to reset timer
	rf.resetTimer()
	// make sure that snapshot isn't out-of-date
	lastTerm := -1
	if args.LastIncludedIndex >= rf.lastIncludedIndex &&
		args.LastIncludedIndex < len(rf.log)+rf.lastIncludedIndex {
		lastTerm = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex].Term
	}
	if rf.lastIncludedIndex >= args.LastIncludedIndex || lastTerm == args.LastIncludedTerm {
		rf.D.Printf("%s reject InstallSnapshot request %s", rf, args)
		return
	}
	// change the index
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// reset states!
	rf.commitIndex = MaxInt(args.LastIncludedIndex, rf.commitIndex)
	rf.log = append(make([]LogEntry, 0), LogEntry{
		Term: args.LastIncludedTerm,
	})
	// persist state and snapshot
	rf.persistStateAndSnapshot(args.Data)
	rf.applyCond.Signal()
}

func (rf *Raft) issueInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the term
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		// TODO: does it need to reset timer?
		// I believe it is needed
		// based on Figure 3 in raft paper
		//rf.resetTimer()
	}
	// change the next index of corresponding server
	// make sure that the server's state is not changed
	// first check the state and the term hasn't change
	if args.Term != rf.currentTerm || rf.state != LEADER {
		rf.D.Printf("%v is not leader or changes from previous term: %v", rf, args.Term)
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.D.Printf("%s update raft peer %d: [matchIndex:%d;nextIndex:%d]",
		rf, server, rf.matchIndex[server], rf.nextIndex[server])
}

//
// send leader's latest entries to follow
//
func (rf *Raft) sendEntry(peerID int) {
	if peerID == rf.me {
		rf.nextIndex[peerID] = len(rf.log) + rf.lastIncludedIndex
		rf.matchIndex[peerID] = rf.nextIndex[peerID] - 1
		return
	} else {
		if rf.nextIndex[peerID] <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LastIncludedIndex = rf.lastIncludedIndex
			args.LastIncludedTerm = rf.lastIncludedTerm
			args.Data = rf.persister.ReadSnapshot()
			rf.D.Printf("%v send InstallSnapshot %s to peer %v", rf, &args, peerID)
			reply := InstallSnapshotReply{}
			go rf.issueInstallSnapshot(peerID, &args, &reply)
		} else {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[peerID] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1-rf.lastIncludedIndex:]
			rf.D.Printf("%v send AppendEntries %s to peer %v", rf, &args, peerID)
			var reply AppendEntriesReply
			go rf.issueAppendEntries(peerID, &args, &reply)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeats(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Start starts agreement on the next Data to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Data will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Data will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.killed() {
		rf.D.Printf("Raft peer %v has been killed", rf.me)
		return index, term, false
	}

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.isLeader()
	if !isLeader {
		rf.D.Printf("%v is not a leader", rf)
		// return index, term, isLeader
	} else {
		index = len(rf.log) + rf.lastIncludedIndex
		term = rf.currentTerm
		rf.D.Printf("%v receive new command", rf)
		// append a new log entry to local log
		rf.log = append(rf.log, LogEntry{
			Term: rf.currentTerm,
			Data: command,
		})
		rf.persist()

		for i := 0; i < len(rf.peers); i++ {
			rf.sendEntry(i)
		}
	}

	return index, term, isLeader
}

// the replicate state machine call this function to save snapshot
func (rf *Raft) SaveSnapshot(index int, data []byte, maxraftstate int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ignore out-of-date changes
	if rf.lastIncludedIndex < index && rf.persister.RaftStateSize() > maxraftstate {
		rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
		// we doesn't remove the last log as a dummy head
		rf.log = append(make([]LogEntry, 0), rf.log[index-rf.lastIncludedIndex:]...)
		rf.lastIncludedIndex = index
		// save the change
		rf.persistStateAndSnapshot(data)
	}
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.D.Printf("%s is killed", rf)
	rf.heartbeatCond.Signal()
	rf.applyCond.Signal()

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	rf.applyCond = sync.Cond{}
	rf.applyCond.L = &rf.mu
	rf.heartbeatCond = sync.Cond{}
	rf.heartbeatCond.L = &rf.mu

	// Your initialization code here (2A, 2B, 2C).

	// initialize internal states
	rf.lastElectionTime = time.Now()
	//rf.lastAppendTime = time.Now()
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.receiveVoteCount = 0
	rf.leaderId = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0

	// reset election timer
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// add a dummy log as a sentinel
	if len(rf.log) == 0 && rf.lastIncludedIndex == 0 {
		rf.log = append(rf.log, LogEntry{
			Term: 0,
			Data: nil,
		})
	}

	// three separate loop
	// reference: https://zhuanlan.zhihu.com/p/103849249 (THANKS!)
	go rf.electionLoop()
	go rf.heartbeatLoop()
	go rf.applyLoop()

	return rf
}
