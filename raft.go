package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"log"

	"cs351/labrpc"
)


const (
	CandidateState  =1
	FollowerState = 0
	LeaderState = 2
	
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}



// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // This peer's index into peers[]
	dead  int32               // Set by Kill()

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	state int

	voteCount int
	votedFor int

	logEntries []LogEntry

	matchIndx []int
	nextIndx []int
	heartBeat bool

	commitIndx int

	applyChnlBuff chan ApplyMsg

	applyChnl chan ApplyMsg

	lastApplied int

}

type LogEntry struct {
	Term int //log rterm 

	Index int //index (glaobl)
	Entry interface{} //log interface
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	LeaderCommitIndx int
	PreviousLogTerm int
	PreviousLogIndx int
	LogEntries []LogEntry

}

type AppendEntryReply struct {
	Success bool //was the entry appeneded (accepted or declined)
	Term int //term of htre server receiving the appendentry request
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateId int //if there are n servers then candidate id from 0 -> n-1
	Term int //(current)term of the candidate

	//3B
	LastLogTerm int
	LastLogIndx int

}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGiven bool 
}


func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LeaderState)
	rf.mu.Unlock()
	return term, isleader
}



func (rf *Raft) getLastTerm() int {
	// if len(rf.logEntries) > 0 {
	// 	lastIndex := len(rf.logEntries) - 1
	// 	return rf.logEntries[lastIndex].Term
	// } else {
	// 	return -1
	// }
	return rf.logEntries[len(rf.logEntries)-1].Term
	
}

func (rf *Raft) getLastIndex() int {
	return rf.logEntries[len(rf.logEntries)-1].Index
	// if len(rf.logEntries) > 0 {
	// 	lastIndex := len(rf.logEntries) - 1
	// 	return lastIndex
	// } else {
	// 	return -1
	// }
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	reply.VoteGiven = false
	lastIndx := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()

	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
		
	}
	if rf.currentTerm > args.Term {
		reply.VoteGiven = false
		reply.Term = rf.currentTerm

		return
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndx >= lastIndx)) {
				rf.votedFor = args.CandidateId
				reply.VoteGiven = true

	}
}

func minimum(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true

	reply.Term = args.Term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	} else if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.voteCount = 0
		rf.currentTerm = args.Term
	}

	if args.Term >= rf.currentTerm {
		rf.heartBeat = true
	}

	if args.PreviousLogIndx >= 1 {

		if args.PreviousLogIndx < rf.logEntries[0].Index {
			reply.Success = false
			return
		}

		if args.PreviousLogIndx > rf.logEntries[len(rf.logEntries)-1].Index {
			reply.Success = false

			return
		} else if args.PreviousLogTerm != rf.logEntries[args.PreviousLogIndx-rf.logEntries[0].Index].Term {
			reply.Success = false
			//
			return
		} else {
			rf.logEntries = rf.logEntries[:args.PreviousLogIndx+1-rf.logEntries[0].Index]
			rf.logEntries = append(rf.logEntries, args.LogEntries...)
			reply.Success = true
		}
	} else if args.PreviousLogIndx == 0 {
		rf.logEntries = []LogEntry{{0, 0, 0}}
		rf.logEntries = append(rf.logEntries, args.LogEntries...)
		reply.Success = true

	} else if args.PreviousLogIndx == -1 {
		rf.logEntries = args.LogEntries
		reply.Success = true

	} else if len(args.LogEntries) != 0 {
		rf.logEntries = append(rf.logEntries, args.LogEntries...)
		reply.Success = true
	}

	if args.LeaderCommitIndx > rf.commitIndx {
		N := minimum(args.LeaderCommitIndx, args.PreviousLogIndx+len(rf.logEntries))

		for i := rf.commitIndx + 1; i <= N; i++ {
			rf.commitIndx = i

			rf.applyChnlBuff <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[i-rf.logEntries[0].Index].Entry,
				CommandIndex: i,
			}

		}
	}
}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) { 
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	good := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	if good { 
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteCount = 0
			rf.votedFor = -1
			rf.state = FollowerState
			rf.mu.Unlock()
			return good

		} else if !reply.Success {
			rf.nextIndx[server] = 1
			
			rf.mu.Unlock()
			return good
		}

		rf.matchIndx[server] = len(args.LogEntries) + args.PreviousLogIndx
		rf.nextIndx[server] = rf.matchIndx[server] + 1
		rf.mu.Unlock()
		
		log.Println("After send append entry", server, rf.matchIndx[server], rf.nextIndx[server])
		rf.checkCommits()
	}
	return good
}


func (rf *Raft) checkCommits() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("[%d] Commit_Checker running\n", rf.me)

	majority := len(rf.peers) / 2
	for I := rf.commitIndx + 1; I <= rf.logEntries[len(rf.logEntries)-1].Index; I++ {
		counter := 0
		for _, index := range rf.matchIndx {
			if index >= I {
				counter++
			}
			if counter > majority {
				break
			}
		}
		if counter > majority && rf.logEntries[I-rf.logEntries[0].Index].Term == rf.currentTerm {
			for j := rf.commitIndx + 1; j <= I; j++ {
				rf.commitIndx = j

				rf.applyChnlBuff <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[j-rf.logEntries[0].Index].Entry,
					CommandIndex: j,
				}

			}
			break
		}
	}
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	indx := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LeaderState {
		term = rf.currentTerm
		isLeader = true
		indx = rf.logEntries[len(rf.logEntries)-1].Index + 1
		log := LogEntry{
			Term:  term,
			Entry: command,
			Index: indx,
		}
		rf.logEntries = append(rf.logEntries, log)
		rf.matchIndx[rf.me] = indx
		rf.nextIndx[rf.me] = indx + 1
		//
		return indx, term, isLeader
	}
	isLeader = false

	return indx + 1, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) requestServersVote(server int) {
	// log.Printf("bkl")
	rf.mu.Lock()
	if rf.state != 1 || rf.killed() {
		rf.mu.Unlock()
		return
	}


	if server != rf.me {
		args := RequestVoteArgs{
			CandidateId: rf.me,
			Term:        rf.currentTerm}

		args.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		args.LastLogIndx = rf.logEntries[len(rf.logEntries)-1].Index

		reply := RequestVoteReply{}
		rf.mu.Unlock()
		ok := rf.sendRequestVote(server, &args, &reply)
		rf.mu.Lock()

		if !ok {
			rf.mu.Unlock()

			return
		}
		if rf.state != CandidateState {
			rf.mu.Unlock()

			return
		}

		if reply.VoteGiven {
			rf.voteCount++
		} else if reply.Term > rf.currentTerm {
			
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.voteCount = 0
			rf.votedFor = -1
		}
	}
	if rf.voteCount > int(len(rf.peers)/2) {
		//change the state and reset votecount to 0 and then have to use for loop to reset matchIndex to 0 and nextIndx to last log idx + !of leader
		rf.state = 2
		rf.voteCount = 0
		
		for j := 0; j < len(rf.nextIndx); j++ {
			rf.nextIndx[j] = rf.logEntries[len(rf.logEntries)-1].Index + 1
			rf.matchIndx[j] = 0
		}

		// log.Printf("beofre handleleader")
		go rf.leaderHandler()
	}
	rf.mu.Unlock()
}

func (rf *Raft) dilKiDhadkanSambhal(server int) { // sending append entries
	// log.Printf("In DilkiDhadkanSambhal")
	rf.mu.Lock()

	if rf.nextIndx[server] > rf.logEntries[0].Index {
		loggg := make([]LogEntry, len(rf.logEntries))
		copy(loggg, rf.logEntries)

		args := AppendEntryArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			LeaderCommitIndx: rf.commitIndx,
		}
		if rf.nextIndx[server] <= 0 {
			args.LogEntries = loggg
			args.PreviousLogIndx = -1
			rf.nextIndx[server] = 0
		} else if rf.nextIndx[server] > rf.logEntries[len(rf.logEntries)-1].Index {
			args.LogEntries = []LogEntry{}
			args.PreviousLogIndx = rf.nextIndx[server] - 1
			args.PreviousLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		} else {
			args.LogEntries = loggg[rf.nextIndx[server]-rf.logEntries[0].Index:]
			args.PreviousLogIndx = rf.nextIndx[server] - 1
			if args.PreviousLogIndx >= 1 {
				args.PreviousLogTerm = loggg[args.PreviousLogIndx-rf.logEntries[0].Index].Term
			}
		}

		reply := AppendEntryReply{}
		rf.mu.Unlock()
		good := rf.sendAppendEntries(server, &args, &reply)
		rf.mu.Lock()
		if !good || rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.voteCount = 0
			rf.votedFor = -1
		}
		// if reply = sucess
		rf.mu.Unlock()
	} else {

		rf.mu.Unlock()
	}
}


func (rf *Raft) leaderHandler() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		for mate := range rf.peers {
			if mate != rf.me {
				go rf.dilKiDhadkanSambhal(mate)
			}
		}
		rf.mu.Unlock()
		time.Sleep(90 * time.Millisecond)
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeRand := time.Duration(rand.Intn(150)+150) * time.Millisecond
		time.Sleep(electionTimeRand)
		rf.mu.Lock()
		// log.Printf("[%d] commitIndex: %d", rf.me, rf.commitIndex)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.heartBeat || rf.state == 2 {
			rf.heartBeat = false
		} else {

			rf.state = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			for peer := range rf.peers {
				// log.Printf("in ticker before requestvote")
				go rf.requestServersVote(peer)
			}

		}
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = 0
	rf.voteCount = 0
	rf.heartBeat = false
	rf.lastApplied = 0
	rf.commitIndx = 0
	rf.logEntries = []LogEntry{{0, 0, 0}}

	rf.nextIndx = make([]int, len(rf.peers))

	for i := 0; i < len(rf.nextIndx); i++ {
		rf.nextIndx[i] = len(rf.logEntries)
	}
	rf.matchIndx = make([]int, len(rf.peers))

	rf.applyChnl = applyCh
	rf.applyChnlBuff = make(chan ApplyMsg, 1000)

	applyMessege := ApplyMsg{
		CommandValid: true,
		Command:      rf.logEntries[0].Entry,
		CommandIndex: 0,
	}
	rf.applyChnlBuff <- applyMessege

	for i, _ := range rf.nextIndx {
		rf.nextIndx[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
	}

	if rf.commitIndx < rf.logEntries[0].Index {
		rf.commitIndx = rf.logEntries[0].Index

	}

	// start ticker - electiond
	go rf.ticker()
	go rf.sendBufferToApplyCh()

	return rf
}

func (rf *Raft) sendBufferToApplyCh() {
	for !rf.killed() {
		applyMessege := <-rf.applyChnlBuff
		rf.applyChnl <- applyMessege
	}
}

//**************************************************************************************************
//#ignore
// // Return currentTerm and whether this server
// // believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {
// 	var term int
// 	var isleader bool
// 	// Your code here (3A).
// 	rf.mu.Lock()
// 	term = rf.currentTerm
// 	isleader = (rf.state == LeaderState)
// 	rf.mu.Unlock()
// 	// return term, validLeader

// 	return term, isleader
// }

// func (rf *Raft) getLastIndx() int {
// 	// if rf.killed(){
// 	// 	return
// 	// }
// 	// if len(rf.logEntries) > 0 {
// 	// 	lastIndex := len(rf.logEntries) - 1
// 	// 	return rf.logEntries[lastIndex].Index  
// 	// } else {
// 	// 	return -1  
// 	// }
// 	return rf.logEntries[len(rf.logEntries)-1].Index

// }

// func (rf *Raft) getLastlogTerm() int {
// 	// if len(rf.logEntries) > 0 {
// 	// 	lastTerm := len(rf.logEntries) - 1
// 	// 	return rf.logEntries[lastTerm].Term  
// 	// } else {
// 	// 	return -1  
// 	// }
// 	return rf.logEntries[len(rf.logEntries)-1].Term
// }


// // Example RequestVote RPC arguments structure.
// // Field names must start with capital letters!
// // type RequestVoteArgs struct {
// // 	// Your data here (3A, 3B).
// // 	CandidateId int //if there are n servers then candidate id from 0 -> n-1
// // 	Term int //(current)term of the candidate

// // 	//3B
// // 	LastLogTerm int
// // 	LastLogIndx int

// // }

// // // Example RequestVote RPC reply structure.
// // // Field names must start with capital letters!
// // type RequestVoteReply struct {
// // 	// Your data here (3A).
// // 	Term int
// // 	VoteGiven bool 
// // }


// // Example RequestVote RPC handler.
// // func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// // 	if rf.killed(){
// // 		return
// // 	}
// // 	// Your code here (3A, 3B).

// // 	rf.mu.Lock()
// // 	defer rf.mu.Unlock()

// // 	reply.Term = rf.currentTerm
// // 	reply.VoteGiven = false
// // 	//code to check log indices will add later (part B)
// // 	lastlogTerm := rf.getLastlogTerm()
// // 	lastIndx := rf.getLastIndx() //will add these methods later 

// // 	if (args.Term > rf.currentTerm){
// // 		rf.state = FollowerState
// // 		rf.currentTerm = args.Term
// // 		rf.votedFor = -1
// // 		rf.voteCount = 0 
// // 	}

// // 	//we dont vote when our term > requesters ter,
// // 	if (rf.currentTerm > args.Term) {
// // 		reply.VoteGiven = false
// // 		reply.Term = rf.currentTerm
// // 		return
// // 	}

// // 	// if (rf.currentTerm == args.Term && rf.votedFor == -1 && (args.LastLogTerm> lastlogTerm || (args.LastLogTerm == lastlogTerm && args.LastLogIndx >= lastIndx))){ //will add the logs logic later
// // 	// 	reply.VoteGiven = true
// // 	// 	rf.votedFor = args.CandidateId
// // 	// }
// // 	if rf.currentTerm == args.Term &&
// //     (rf.votedFor == -1) &&
// //     (args.LastLogTerm > lastlogTerm ||
// //      (args.LastLogTerm == lastlogTerm && args.LastLogIndx >= lastIndx)) {
// // 		reply.VoteGiven = true
// // 		rf.votedFor = args.CandidateId
// // 	 }
// // }


// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	if rf.killed() {
// 		return
// 	}

// 	// reply.Term = args.Term
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	//
// 	reply.Term = rf.currentTerm
// 	reply.VoteGiven = false
// 	lastLogIndex := rf.getLastIndx()
// 	lastLogTerm := rf.getLastlogTerm()

// 	if args.Term > rf.currentTerm {
// 		rf.state = FollowerState
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.voteCount = 0
// 		//
// 	}
// 	if rf.currentTerm > args.Term {
// 		reply.VoteGiven = false
// 		reply.Term = rf.currentTerm
// 		//
// 		return
// 	}
// 	//
// 	if rf.currentTerm == args.Term &&
// 		(rf.votedFor == -1) &&
// 		(args.LastLogTerm > lastLogTerm ||
// 			(args.LastLogTerm == lastLogTerm && args.LastLogIndx >= lastLogIndex)) { // good
// 		reply.VoteGiven = true
// 		rf.votedFor = args.CandidateId
// 		//
// 	}
// }


// func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
// 	if rf.killed() {
// 		return
// 	}
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	reply.Success = true

// 	reply.Term = args.Term //rf.currentTerm
// 	if args.Term < rf.currentTerm {
// 		reply.Success = false
// 		reply.Term = rf.currentTerm

// 		return
// 	} else if args.Term > rf.currentTerm {
// 		rf.state = FollowerState
// 		rf.votedFor = -1
// 		rf.voteCount = 0
// 		rf.currentTerm = args.Term
// 	}
// 	if args.Term >= rf.currentTerm {
// 		rf.heartBeat = true
// 	}

// 	if args.PreviousLogIndx >= 1 {

// 		if args.PreviousLogIndx < rf.logEntries[0].Index {
// 			// for unreliable test (leader might not have received my installSS reply)
// 			reply.Success = false
// 			return
// 		}

// 		if args.PreviousLogIndx > rf.logEntries[len(rf.logEntries)-1].Index {
// 			reply.Success = false

// 			return
// 		} else if args.PreviousLogTerm != rf.logEntries[args.PreviousLogIndx-rf.logEntries[0].Index].Term {
// 			reply.Success = false
// 			//
// 			return
// 		} else {
// 			rf.logEntries = rf.logEntries[:args.PreviousLogIndx+1-rf.logEntries[0].Index]
// 			rf.logEntries = append(rf.logEntries, args.LogEntries...)
// 			reply.Success = true
// 		}
// 	} else if args.PreviousLogIndx == 0 {
// 		rf.logEntries = []LogEntry{{0, 0, 0}}
// 		rf.logEntries = append(rf.logEntries, args.LogEntries...)
// 		reply.Success = true
// 	} else if args.PreviousLogIndx == -1 {
// 		rf.logEntries = args.LogEntries
// 		reply.Success = true

// 	} else if len(args.LogEntries) != 0 {
// 		rf.logEntries = append(rf.logEntries, args.LogEntries...)
// 		reply.Success = true
// 	}
// 	if args.LeaderCommitIndx > rf.commitIndx {
// 		N := minimum(args.LeaderCommitIndx, args.PreviousLogIndx+len(rf.logEntries))

// 		for i := rf.commitIndx + 1; i <= N; i++ {
// 			rf.commitIndx = i

// 			rf.applyChnlBuff <- ApplyMsg{
// 				CommandValid: true,
// 				Command:      rf.logEntries[i-rf.logEntries[0].Index].Entry,
// 				CommandIndex: i,
// 			}

// 		}
// 	}
// }


// func minimum(x int, y int) int {
// 	if (x<y) {
// 		return x
// 	} else {
// 		return y
// 	}
// 	// return y
// }

// // func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply){
// // 	if (rf.killed()) {
// // 		return
// // 	}
	
// // 	rf.mu.Lock()
// // 	defer rf.mu.Unlock()

// // 	reply.Success = true

// // 	// reply.Term = args.Term
// // 	if args.Term < rf.currentTerm {
// // 		reply.Success = false
// // 		reply.Term = rf.currentTerm
// // 		return 
// // 	} else if (args.Term > rf.currentTerm) {
// // 		rf.state = FollowerState
// // 		rf.votedFor = -1
// // 		rf.voteCount = 0
// // 		rf.currentTerm = args.Term
// // 	}

// // 	if (args.Term >= rf.currentTerm) {
// // 		rf.heartBeat = true; 
// // 	}

// // 	//logs logic


// // 	// if (args.PreviousLogIndx == 0 ){
// // 	// 	//somecode
// // 	// 	// reply.Success = true
// // 	// 	rf.logEntries = []LogEntry{{0, 0, 0}}
// // 	// 	rf.logEntries = append(rf.logEntries, args.LogEntries...)
// // 	// 	reply.Success = true
// // 	// }

// // 	if (args.PreviousLogIndx >= 1) {
// // 		if (args.PreviousLogIndx < rf.logEntries[0].Index){ //If the prev log indx is less than the current's log indx
// // 			reply.Success = false;
// // 			return
// // 		}

// // 		if (args.PreviousLogIndx > rf.logEntries[len(rf.logEntries)-1].Index) {
// // 			reply.Success = false
// // 			return 
// // 		} else if (args.PreviousLogTerm != rf.logEntries[args.PreviousLogIndx-rf.logEntries[0].Index].Term) {
// // 			reply.Success = false
// // 			return 
// // 		} else {
// // 			// reply.Success = true
// // 			rf.logEntries = rf.logEntries[:args.PreviousLogIndx+1-rf.logEntries[0].Index]
// // 			rf.logEntries = append(rf.logEntries, args.LogEntries...)
// // 			reply.Success = true
// // 		}



// // 	} else if (args.PreviousLogIndx == 0 ){
// // 		//somecode
// // 		// reply.Success = true
// // 		rf.logEntries = []LogEntry{{0, 0, 0}}
// // 		rf.logEntries = append(rf.logEntries, args.LogEntries...)
// // 		reply.Success = true
// // 	} else if (args.PreviousLogIndx == -1){
// // 		rf.logEntries = args.LogEntries
// // 		reply.Success = true;
// // 	} else if (len(args.LogEntries) != 0){ 
// // 		// reply.Success = true
// // 		rf.logEntries = append(rf.logEntries, args.LogEntries...)
// // 		reply.Success = true
// // 	}

// // 	if (args.LeaderCommitIndx > rf.commitIndx){
// // 		// var int N;
// // 		N := minimum(args.LeaderCommitIndx, args.PreviousLogIndx+len(rf.logEntries)) //min() = helper function

// // 		for i := rf.commitIndx+1; i <= N; i++ {
// // 			rf.commitIndx = i; 

// // 			rf.applyChnlBuff <- ApplyMsg{
// // 				CommandValid: true,
// // 				Command: rf.logEntries[i-rf.logEntries[0].Index].Entry,
// // 				CommandIndex: i,
// // 			}
// // 		}
		

// // 	}

// // }

// // Example code to send a RequestVote RPC to a server.
// // Server is the index of the target server in rf.peers[].
// // Expects RPC arguments in args. Fills in *reply with RPC reply,
// // so caller should pass &reply.
// //
// // The types of the args and reply passed to Call() must be
// // the same as the types of the arguments declared in the
// // handler function (including whether they are pointers).
// //
// // The labrpc package simulates a lossy network, in which servers
// // may be unreachable, and in which requests and replies may be lost.
// // Call() sends a request and waits for a reply. If a reply arrives
// // within a timeout interval, Call() returns true; otherwise
// // Call() returns false. Thus Call() may not return for a while.
// // A false return can be caused by a dead server, a live server that
// // can't be reached, a lost request, or a lost reply.
// //
// // Call() is guaranteed to return (perhaps after a delay) *except* if the
// // handler function on the server side does not return.  Thus there
// // is no need to implement your own timeouts around Call().
// //
// // Look at the comments in ../labrpc/labrpc.go for more details.
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) { //heartbeat creation
// }


// func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool { //similar to sendRequestVote
// 	good := rf.peers[server].Call("Raft.AppendEntry", args, reply) 

// 	if (good) {
// 		rf.mu.Lock()
// 		// defer rf.mu.Unlock()
// 		if ( reply.Term > rf.currentTerm) {
// 			rf.state = FollowerState
// 			rf.currentTerm = reply.Term
// 			rf.votedFor = -1
// 			rf.voteCount = 0
// 			rf.mu.Unlock()
// 			return good
// 		} else if (!reply.Success){
// 			rf.nextIndx[server] = 1
// 			rf.mu.Unlock()
// 			return good
// 		} 

// 		log.Printf("In sendAppendEntries if success")
// 		rf.matchIndx[server] = len(args.LogEntries) + args.PreviousLogIndx
// 		rf.nextIndx[server] = rf.matchIndx[server] + 1
// 		rf.mu.Unlock()
// 		log.Println("In sendAppendEntries: After send appedn entry", server, rf.matchIndx[server], rf.nextIndx[server])
			
// 		rf.checkCommits() //TODO (later)

// 	}
// 	return good
// }	



// func (rf *Raft) checkCommits() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	log.Printf("[%d]: In commit checker", rf.me)

// 	majority := len(rf.peers)/2
// 	// length := len(rf.logEntries)-1
// 	for I:=	rf.commitIndx+1; I <= rf.logEntries[len(rf.logEntries)-1].Index; I++ {
// 		counter := 0;
// 		for _, id := range rf.matchIndx {
// 			// if counter > majority {
// 			// 	break; 
// 			// }
// 			// if id >= I {
// 			// 	counter++; 
// 			// }
// 			if id >= I {
// 				counter++
// 			} 
// 			if counter > majority {
// 				break
// 			}
// 		}

// 		if counter > majority && rf.logEntries[I-rf.logEntries[0].Index].Term == rf.currentTerm {
// 			for j := rf.commitIndx+1; j <= I; j++ {
// 				rf.commitIndx = j

// 				rf.applyChnlBuff <- ApplyMsg{CommandValid: true, Command: rf.logEntries[j-rf.logEntries[0].Index].Entry, CommandIndex: j}
// 			}
// 			break;
// 		}
// 	}
// }



// // The service using Raft (e.g. a k/v server) wants to start
// // agreement on the next command to be appended to Raft's log. If this
// // server isn't the leader, returns false. Otherwise start the
// // agreement and return immediately. There is no guarantee that this
// // command will ever be committed to the Raft log, since the leader
// // may fail or lose an election. Even if the Raft instance has been killed,
// // this function should return gracefully.
// //
// // The first return value is the index that the command will appear at
// // if it's ever committed. The second return value is the current
// // term. The third return value is true if this server believes it is
// // the leader.
// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	indx := -1
// 	term := -1
// 	isLeader := true

// 	// Your code here (3B).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if (rf.state == LeaderState) {
// 		isLeader =true
// 		term= rf.currentTerm
// 		// length := len(rf.logEntries)-1
// 		indx = rf.logEntries[len(rf.logEntries)-1].Index + 1
// 		log := LogEntry{
// 			Term: term,
// 			Entry: command, 
// 			Index: indx, 
// 		}

// 		rf.logEntries = append(rf.logEntries, log)
// 		rf.matchIndx[rf.me] = indx
// 		rf.nextIndx[rf.me] = indx+1
// 		return indx, term, isLeader

// 	}

// 	isLeader = false
// 	return indx+1, term, isLeader 


// 	// return index, term, isLeader
// }

// // The tester doesn't halt goroutines created by Raft after each test,
// // but it does call the Kill() method. Your code can use killed() to
// // check whether Kill() has been called. The use of atomic avoids the
// // need for a lock.
// //
// // The issue is that long-running goroutines use memory and may chew
// // up CPU time, perhaps causing later tests to fail and generating
// // confusing debug output. Any goroutine with a long-running loop
// // should call killed() to check whether it should stop.
// func (rf *Raft) Kill() {
// 	atomic.StoreInt32(&rf.dead, 1)
// 	// Your code here, if desired.
// }

// func (rf *Raft) killed() bool {
// 	z := atomic.LoadInt32(&rf.dead)
// 	return z == 1
// }

// func (rf *Raft) requestServersVote(server int) {
// 	// log.Printf("Yaha in requestServer vote")
// 	rf.mu.Lock()
// 	if rf.state != 1 || rf.killed() {
// 		rf.mu.Unlock()
// 		return
// 	}
// 	//

// 	if server != rf.me {
// 		//
// 		args := RequestVoteArgs{
// 			CandidateId: rf.me,
// 			Term:        rf.currentTerm}

// 		args.LastLogIndx = rf.logEntries[len(rf.logEntries)-1].Index
// 		args.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term

// 		reply := RequestVoteReply{}
// 		rf.mu.Unlock()
// 		ok := rf.sendRequestVote(server, &args, &reply)
// 		rf.mu.Lock()

// 		if !ok {
// 			rf.mu.Unlock()
// 			//
// 			return
// 		}
// 		if rf.state != CandidateState {
// 			rf.mu.Unlock()
// 			//
// 			return
// 		}

// 		if reply.VoteGiven {
// 			rf.voteCount++
// 		} else if reply.Term > rf.currentTerm {
// 			//
// 			rf.currentTerm = reply.Term
// 			rf.state = 0
// 			rf.voteCount = 0
// 			rf.votedFor = -1
// 		}
// 	}
// 	if rf.voteCount > int(len(rf.peers)/2) {

// 		rf.state = 2
// 		rf.voteCount = 0
// 		// have to reset for loop to reinitialized matchIndex to 0 and nextIndex to initialized to leader last log index + 1
// 		for i := 0; i < len(rf.nextIndx); i++ {
// 			rf.nextIndx[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
// 			rf.matchIndx[i] = 0
// 		}

// 		go rf.leaderHandler()
// 	}
// 	rf.mu.Unlock()
// }


// func (rf *Raft) dilKiDhadkanSambhal(server int) { //heartbeathandler
// 	log.Printf("In DilkiDhadkanSambhal")
// 	rf.mu.Lock()

// 	if rf.nextIndx[server] > rf.logEntries[0].Index {
// 		deep_copy_log := make([]LogEntry, len(rf.logEntries))
// 		copy(deep_copy_log, rf.logEntries)

// 		args := AppendEntryArgs{
// 			LeaderId:     rf.me,
// 			Term:         rf.currentTerm,
// 			LeaderCommitIndx: rf.commitIndx,
// 		}
// 		if rf.nextIndx[server] <= 0 {
// 			args.LogEntries = deep_copy_log
// 			args.PreviousLogIndx = -1
// 			rf.nextIndx[server] = 0
// 		} else if rf.nextIndx[server] > rf.logEntries[len(rf.logEntries)-1].Index {
// 			args.LogEntries = []LogEntry{}
// 			args.PreviousLogIndx = rf.nextIndx[server] - 1
// 			args.PreviousLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
// 		} else {
// 			args.LogEntries = deep_copy_log[rf.nextIndx[server]-rf.logEntries[0].Index:]
// 			args.PreviousLogIndx = rf.nextIndx[server] - 1
// 			if args.PreviousLogIndx >= 1 {
// 				args.PreviousLogTerm = deep_copy_log[args.PreviousLogIndx-rf.logEntries[0].Index].Term
// 			}
// 		}

// 		reply := AppendEntryReply{}
// 		rf.mu.Unlock()
// 		ok := rf.sendAppendEntries(server, &args, &reply)
// 		rf.mu.Lock()
// 		if !ok || rf.state != 2 {
// 			//
// 			rf.mu.Unlock()
// 			return
// 		}
// 		if reply.Term > rf.currentTerm {
// 			rf.currentTerm = reply.Term
// 			rf.state = 0
// 			rf.voteCount = 0
// 			rf.votedFor = -1
// 			//
// 		}
// 		// if reply = sucess
// 		rf.mu.Unlock()
// 	} else {
// 		// follower is lagging
// 		rf.mu.Unlock()
// 	}
// 	// rf.mu.Lock()

// 	// if (rf.nextIndx[server] > rf.logEntries[0].Index) { 
// 	// 	loggg := make([]LogEntry, len(rf.logEntries))
// 	// 	copy(loggg, rf.logEntries)
// 	// 	// args := AppendEntry{LeaderId: rf.me, Term: rf.currentTerm, LeaderCommitIndx: rf.commitIndx}
// 	// 	// length := len(rf.logEntries)-1

// 	// 	args := AppendEntryArgs{
// 	// 		LeaderId: rf.me,
// 	// 		Term: rf.currentTerm, 
// 	// 		LeaderCommitIndx: rf.commitIndx,
// 	// 	}

// 	// 	if rf.nextIndx[server] <= 0 {
// 	// 		args.LogEntries = loggg
// 	// 		args.PreviousLogIndx = -1
// 	// 		rf.nextIndx[server] = 0
// 	// 	} else if (rf.nextIndx[server] > rf.logEntries[len(rf.logEntries)-1].Index) {
// 	// 		args.LogEntries = []LogEntry{}
// 	// 		args.PreviousLogIndx = rf.nextIndx[server] -1
// 	// 		args.PreviousLogTerm = rf.logEntries[len(rf.logEntries)-1].Term

// 	// 	// } else if (rf.nextIndx[server] <= 0){
// 	// 	// 	args.PreviousLogIndx = -1
// 	// 	// 	rf.nextIndx[server] = 0
// 	// 	} else {
// 	// 		args.LogEntries = loggg[rf.nextIndx[server]-rf.logEntries[0].Index:]
// 	// 		args.PreviousLogIndx = rf.nextIndx[server] -1
// 	// 		if (args.PreviousLogIndx >= 1) {
// 	// 			//
// 	// 			args.PreviousLogTerm = loggg[args.PreviousLogIndx-rf.logEntries[0].Index].Term
// 	// 		}
// 	// 	}

// 	// 	reply := AppendEntryReply{}
// 	// 	rf.mu.Unlock()
// 	// 	good := rf.sendAppendEntries(server, &args, &reply)
// 	// 	rf.mu.Lock()
// 	// 	if (!good || rf.state != 2) {
// 	// 		rf.mu.Unlock()
// 	// 		return
// 	// 	} 
// 	// 	if (rf.currentTerm < reply.Term) {
// 	// 		// rf.state = 0
// 	// 		// rf.currentTerm = reply.Term
// 	// 		// rf.votedFor = -1
// 	// 		// rf.voteCount = 0 
// 	// 		rf.currentTerm = reply.Term
// 	// 		rf.state = 0
// 	// 		rf.voteCount = 0
// 	// 		rf.votedFor = -1
			
// 	// 	}
// 	// 	rf.mu.Unlock()
		
// 	// } else {
// 	// 	rf.mu.Unlock()
// 	// }

// }



// // func (rf *Raft) dilKiDhadkanSambhal(server int) { // Sending append entries (heartbeat handler)
// //     log.Printf("In DilkiDhadkanSambhal")
// //     rf.mu.Lock()

// //     if rf.nextIndx[server] > rf.logEntries[0].Index {
// //         args := AppendEntry{
// //             LeaderId:        rf.me,
// //             Term:            rf.currentTerm,
// //             LeaderCommitIndx: rf.commitIndx,
// //         }

// //         if rf.nextIndx[server] > rf.logEntries[len(rf.logEntries)-1].Index {
// //             args.LogEntries = []LogEntry{}
// //             args.PreviousLogIndx = rf.nextIndx[server] - 1
// //             args.PreviousLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
// //         } else if rf.nextIndx[server] <= 0 {
// //             args.PreviousLogIndx = -1
// //             rf.nextIndx[server] = 0
// //         } else {
// //             args.LogEntries = rf.logEntries[rf.nextIndx[server]-rf.logEntries[0].Index:]
// //             args.PreviousLogIndx = rf.nextIndx[server] - 1
// //             if args.PreviousLogIndx >= 1 {
// //                 args.PreviousLogTerm = rf.logEntries[args.PreviousLogIndx-rf.logEntries[0].Index].Term
// //             }
// //         }

// //         reply := AppendEntryReply{}
// //         rf.mu.Unlock()
// //         good := rf.sendAppendEntries(server, &args, &reply)
// //         rf.mu.Lock()
// //         if !good || rf.state != 2 {
// //             rf.mu.Unlock()
// //             return
// //         }
// //         if rf.currentTerm < reply.Term {
// //             rf.currentTerm = reply.Term
// //             rf.state = 0
// //             rf.voteCount = 0
// //             rf.votedFor = -1
// //         }
// //     }

// //     rf.mu.Unlock()
// // }


// func (rf *Raft) leaderHandler() {
// 	log.Printf("In handleLeader")
// 	if !rf.killed() {
// 		rf.mu.Lock()

// 		if (rf.state != 2) { //not a leader
// 			rf.mu.Unlock();
// 			return

// 		}

// 		for mate := range rf.peers {
// 			if (mate != rf.me) {
// 				go rf.dilKiDhadkanSambhal(mate) // handles heartbeats

// 			}
// 		}

// 		rf.mu.Unlock()
// 		log.Printf("Sone ke pehle bkl")
// 		time.Sleep(90 * time.Millisecond)

// 	}
// }

// // The ticker go routine starts a new election if this peer hasn't received
// // heartsbeats recently.
// func (rf *Raft) ticker() {
// 	for !rf.killed() {

// 		// Your code here to check if a leader election should
// 		// be started and to randomize sleeping time using
// 		// time.Sleep().

// 		electionTimeRand := time.Duration(rand.Intn(901)+900)*time.Millisecond
// 		time.Sleep(electionTimeRand)

// 		rf.mu.Lock()
// 		if (rf.heartBeat || rf.state == 2) {
// 			rf.heartBeat = false
// 		} else {
// 			rf.state = 1
// 			rf.currentTerm++
// 			rf.votedFor = rf.me
// 			rf.voteCount = 1
// 			for peer:= range rf.peers {
// 				log.Printf("in ticker before requestvote")
// 				go rf.requestServersVote(peer)
// 			}
// 		}

// 		rf.mu.Unlock()


// 	}
// }

// // The service or tester wants to create a Raft server. The ports
// // of all the Raft servers (including this one) are in peers[]. This
// // server's port is peers[me]. All the servers' peers[] arrays
// // have the same order. applyCh is a channel on which the
// // tester or service expects Raft to send ApplyMsg messages.
// // Make() must return quickly, so it should start goroutines
// // for any long-running work.
// func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
// 	rf := &Raft{}
// 	rf.peers = peers
// 	rf.me = me

// 	rf.currentTerm = 0 
// 	rf.votedFor = -1 
// 	rf.state = 0
// 	rf.voteCount = 0
// 	rf.heartBeat = false

// 	// Your initialization code here (3A, 3B).

// 	rf.lastApplied = 0
// 	rf.commitIndx = 0
// 	rf.logEntries = []LogEntry{{0, 0, 0}}

// 	rf.nextIndx = make([]int, len(rf.peers))

// 	for i := 0; i < len(rf.nextIndx); i++ {
// 		rf.nextIndx[i] = len(rf.logEntries)
// 	}
// 	rf.matchIndx = make([]int, len(rf.peers))

// 	rf.applyChnl = applyCh
// 	rf.applyChnlBuff = make(chan ApplyMsg, 1000)

// 	applyMessege := ApplyMsg{
// 		CommandValid: true,
// 		Command:      rf.logEntries[0].Entry,
// 		CommandIndex: 0,
// 	}
// 	rf.applyChnlBuff <- applyMessege

// 	for i, _ := range rf.nextIndx {
// 		rf.nextIndx[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
// 	}

// 	// if rf.commitIndx < rf.logEntries[0].Index {
// 	// 	rf.commitIndx = rf.logEntries[0].Index

// 	// }
// 	log.Printf("In make")

// 	// start ticker goroutine to start elections.
// 	go rf.ticker()
// 	go rf.sendBufferToApplyCh()

// 	return rf
// }

// func (rf *Raft) sendBufferToApplyCh() {
// 	for !rf.killed() {
// 		applyMessege := <-rf.applyChnlBuff
// 		rf.applyChnl <- applyMessege
// 	}
// }
