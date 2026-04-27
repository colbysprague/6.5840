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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const NO_VOTE = -1
const HEARTBEAT_INTERVAL_MS = 100
const ELECTION_TIMEOUT_BASE_MS = 400
const ELECTION_TIMEOUT_JITTER_MS = 200

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	currentTerm int
	votedFor    int
	role        Role

	lastHeartbeat time.Time
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term     int
	LeaderId int
	Success  bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the incoming term is less than our current term, reply no success
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.LeaderId = rf.me
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = NO_VOTE
		rf.role = Follower
		rf.currentTerm = args.Term
	}

	if args.Term == rf.currentTerm {
		rf.role = Follower
	}

	rf.lastHeartbeat = time.Now()
	reply.Success = true
	DPrintf("AppendEntries me=%d got heartbeat from leader=%d at %v", rf.me, args.LeaderId, rf.lastHeartbeat)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("RequestVote: me=%d, term=%d, candidate=%d", rf.me, args.Term, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// no vote as this term is old
	if args.Term < rf.currentTerm {
		DPrintf("RequestVote: me=%d term=%d is old, currentTerm=%d", rf.me, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	// there is a new term that has started
	if args.Term > rf.currentTerm {
		DPrintf("RequestVote: me=%d term=%d is new, currentTerm=%d", rf.me, args.Term, rf.currentTerm)
		rf.votedFor = NO_VOTE // reset our vote as we have not voted in this election that is new to us
		rf.role = Follower
		rf.currentTerm = args.Term
	}

	votedFor := rf.votedFor
	if haveNotVotedOrVotedForCandidateAlready(votedFor, args.CandidateId) && candidateLogIsUpToDate() {
		// grant the vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		DPrintf("RequestVote: me=%d granted vote for candidate=%d", rf.me, args.CandidateId)
		return
	}

	// explicitly set vote to false if we fall through
	reply.VoteGranted = false
}

// TODO after 3B+
func candidateLogIsUpToDate() bool {
	return true
}

func haveNotVotedOrVotedForCandidateAlready(votedFor int, candidateId int) bool {
	return votedFor == NO_VOTE || candidateId == votedFor
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (3B).

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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := ELECTION_TIMEOUT_BASE_MS + (rand.Int63() % ELECTION_TIMEOUT_JITTER_MS)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		leader := rf.role == Leader
		rf.mu.Unlock()

		if (time.Since(lastHeartbeat) >= time.Duration(ms)*time.Millisecond) && !leader {
			rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("startElection: me=%d currentTerm=%d", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	// send RequestVote RPCs in parallel
	var votes int = 1
	for peer := range rf.peers {

		if peer == rf.me {
			continue
		}

		go func(id int) {

			rf.mu.Lock()
			DPrintf("startElection: casting vote from me=%d, for peer=%d", rf.me, id)
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(id, args, reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					// abort this election since we are counting results for an old election
					DPrintf("startElection: me=%d my term=%d is stale, peer is on term=%d, reverting to follower", rf.me, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.role = Follower
					return
				}

				if reply.VoteGranted {
					votes++
					if hasMajority(votes, len(rf.peers)) && !isLeader(rf) {
						DPrintf("startElection: me=%d won election, votes=%d, peers=%d", rf.me, votes, len(rf.peers))
						rf.role = Leader
					}
				}
			}
		}(peer)
	}
}

func hasMajority(votes int, peers int) bool {
	return votes >= peers/2+1
}

func isLeader(rf *Raft) bool {
	return rf.role == Leader
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

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = NO_VOTE

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			leader := isLeader(rf)
			rf.mu.Unlock()

			if leader {
				rf.sendHeartbeats()
			}
			time.Sleep(HEARTBEAT_INTERVAL_MS * time.Millisecond)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) sendHeartbeats() {
	success := 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(id, args, reply)

			if ok {
				rf.mu.Lock()
				if reply.Success {
					mu.Lock()
					success++
					mu.Unlock()
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = NO_VOTE
					rf.role = Follower
				}
				rf.mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	rf.mu.Lock()
	if !hasMajority(success, len(rf.peers)) && isLeader(rf) {
		DPrintf("sendHeartbeats: me=%d lost majority, stepping down", rf.me)
		rf.role = Follower
		rf.votedFor = NO_VOTE
	}
	rf.mu.Unlock()
}
