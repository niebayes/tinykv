package raft

import (
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"os"
	"strconv"
	"time"
)

// true to turn on debugging/logging.
const debug = false

// true to turn on logging commands.
const showCommand = false

// true to turn on logging RPC communication events.
const showRPCCommunication = false

// true to turn on logging reset timer events.
const showResetTimer = false

// true to only logging FAIL log consistency check among others.
const showOnlyFailConsistencyCheck = true

// true to turn on logging logs when backup and restore.
const showLog = false

// what topic the log message is related to.
// logs are organized by topics which further consists of events.
type logTopic string

const (
	dElect     logTopic = "ELEC" // leader election events: election timeout, start new election, step down, become leader.
	dVote      logTopic = "VOTE" // voting events: request, grant, deny votes.
	dClient    logTopic = "CLNT" // client interaction events: start server, receive client's agreement request, apply new ApplyMsg to client.
	dReplicate logTopic = "LRPE" // log replication events: leader append new logs, leader detect new logs, leader sends new logs, log consistency check, follower handle conflicts, follower appends new logs, leader updates next index.
	dCommit    logTopic = "CMIT" // log commit events: leader commits, follower commits.
	dMsg       logTopic = "MESG" // RPC communication events: send RPC, receive RPC request, receive RPC response.
	dReject    logTopic = "REJC" // reject RPC events: term changes, state changes.
	dTimer     logTopic = "TIMR" // reset timer events: receive AppendEntries from current leader, start new election, grant vote.
	dPersist   logTopic = "PERS" // persistence events: backup, restore.
	dSnap      logTopic = "SNAP" // snapshotting events: service sends a snapshot, server snapshots, leader detects a follower is lagging hebind, leader sends InstallSnapshot to lagged follower, follower forwards snapshot to service, service conditionally installs a snapshot by asking Raft.
)

type Logger struct {
	logToFile      bool
	logFile        *os.File
	verbosityLevel int // logging verbosity is controlled over environment verbosity variable.
	startTime      time.Time
	r              *Raft
}

func makeLogger(logToFile bool, logFileName string) *Logger {
	logger := &Logger{}
	logger.init(logToFile, logFileName)
	return logger
}

func (logger *Logger) init(logToFile bool, logFileName string) {
	logger.logToFile = logToFile
	logger.verbosityLevel = getVerbosityLevel()
	logger.startTime = time.Now()

	// set log config.
	if logger.logToFile {
		logger.setLogFile(logFileName)
	}
	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime)) // not show date and time.
}

func (logger *Logger) setLogFile(filename string) {
	// FIXME(bayes): What to do with this file if backed up?
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to create file %v", filename)
	}
	log.SetOutput(f)
	logger.logFile = f
}

func (logger *Logger) printf(topic logTopic, format string, a ...interface{}) {
	// print iff debug is set.
	if debug {
		time := time.Since(logger.startTime).Milliseconds()
		// e.g. 008256 VOTE ...
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func (logger *Logger) close() {
	if logger.logToFile {
		err := logger.logFile.Close()
		if err != nil {
			log.Fatal("failed to close log file")
		}
	}
}

//
// leader election events.
//

func (l *Logger) recvHUP() {
	r := l.r
	l.printf(dElect, "N%v <- HUP (T:%v)", r.id, r.Term)
}

func (l *Logger) elecTimeout() {
	r := l.r
	l.printf(dElect, "N%v ETO (S:%v T:%v)", r.id, r.State, r.Term)
}

func (l *Logger) stateToCandidate() {
	r := l.r
	l.printf(dElect, "N%v %v->%v (T:%v)", r.id, r.State, StateCandidate, r.Term)
}

func (l *Logger) bcastRVOT() {
	r := l.r
	l.printf(dElect, "N%v BCAST RVOT (T:%v)", r.id, r.Term)
}

func (l *Logger) recvRVOT(m pb.Message) {
	r := l.r
	l.printf(dElect, "N%v <- N%v RVOT", r.id, m.From)
}

func (l *Logger) recvRVOTRes(m pb.Message) {
	r := l.r
	l.printf(dElect, "N%v <- N%v RVOT RES", r.id, m.From)
}

func (l *Logger) voteTo(to uint64) {
	r := l.r
	l.printf(dElect, "N%v v-> N%v", r.id, to)
}

func (l *Logger) stateToLeader() {
	r := l.r
	l.printf(dElect, "N%v %v->%v (T:%v)", r.id, r.State, StateLeader, r.Term)
}

func (l *Logger) rejectVoteTo(to uint64) {
	r := l.r
	l.printf(dElect, "N%v !v-> N%v", r.id, to)
}

func (l *Logger) stateToFollower(oldTerm uint64) {
	r := l.r
	l.printf(dElect, "N%v %v->%v (LT:%v CT:%v)", r.id, r.State, StateFollower, oldTerm, r.Term)
}

//
// log replication events.
//

func (l *Logger) recvPROP(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- PROP (LN:%v)", r.id, len(m.Entries))
}

func (l *Logger) appendEnts(ents []pb.Entry) {
	r := l.r
	l.printf(dReplicate, "N%v +e (LN:%v)", r.id, len(ents))
	for _, ent := range ents {
		l.printf(dReplicate, "\t(I:%v T:%v D:%v)", ent.Index, ent.Term, ent.Data)
	}
}

func (l *Logger) bcastAENT() {
	r := l.r
	l.printf(dReplicate, "N%v BCAST AENT", r.id)
}

func (l *Logger) sendEnts(ents []pb.Entry, to uint64) {
	r := l.r
	l.printf(dReplicate, "N%v e-> N%v (LN:%v)", r.id, to)
	for _, ent := range ents {
		l.printf(dReplicate, "\t(I:%v T:%v D:%v)", ent.Index, ent.Term, ent.Data)
	}
}

var reasonMap = [...]string{
	"IC", // index conflict.
	"TC", // term conflict.
}

func (l *Logger) rejectEnts(reason pb.RejectReason, from uint64) {
	r := l.r
	l.printf(dReplicate, "N%v !e<- N%v COZ %v", r.id, from, reasonMap[reason])
}

func (l *Logger) discardEnts(ents []pb.Entry) {
	r := l.r
	l.printf(dReplicate, "N%v -e (LN:%v)", r.id, len(ents))
	for _, ent := range ents {
		l.printf(dReplicate, "\t(I:%v T:%v D:%v)", ent.Index, ent.Term, ent.Data)
	}
}

func (l *Logger) updateProgOf(id, oldNext, oldMatch, newNext, newMatch uint64) {
	r := l.r
	l.printf(dReplicate, "N%v PROG OF N%v (NI:%v MI:%v) -> (NI:%v MI:%v)", r.id, id, oldNext, oldMatch, newNext, newMatch)
}

func (l *Logger) updateCommitted(oldCommitted uint64) {
	r := l.r
	l.printf(dReplicate, "N%v (CI:%v) -> (CI:%v)", r.id, r.RaftLog.committed, oldCommitted)
}

func (l *Logger) updateStabled(oldStabled uint64) {
	r := l.r
	l.printf(dReplicate, "N%v (SI:%v) -> (SI:%v)", r.id, r.RaftLog.stabled, oldStabled)
}

func (l *Logger) updateApplied(oldApplied uint64) {
	r := l.r
	l.printf(dReplicate, "N%v (AI:%v) -> (AI:%v)", r.id, r.RaftLog.applied, oldApplied)
}

func (l *Logger) recvAENT(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- N%v AENT (T:%v CI:%v PI:%v PT:%v LN:%v)", r.id, m.From, m.Term, m.Commit, m.Index, m.LogTerm, len(m.Entries))
}

func (l *Logger) recvAENTRes(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- N%v AENT RES (T:%v R:%v RR:%v NI:%v CT:%v)", r.id, m.From, m.Term, m.Reject, m.Reason, m.NextIndex, m.ConflictTerm)
}

func (l *Logger) recvBEAT() {
	r := l.r
	l.printf(dReplicate, "N%v <- BEAT", r.id)
}

func (l *Logger) bcastHBET() {
	r := l.r
	l.printf(dReplicate, "N%v BCAST HBET", r.id)
}

func (l *Logger) beatTimeout() {
	r := l.r
	l.printf(dReplicate, "N%v BTO (S:%v T:%v)", r.id, r.State, r.Term)
}

// 
// persistence event logging.
//

func (l *Logger) restoreEnts(ents []pb.Entry) {
	l.printf(dPersist, "N%v RESTORE ENTS (LN:%v)", len(ents))
	for _, ent := range ents {
		l.printf(dPersist, "\t(I:%v T:%v D:%v)", ent.Index, ent.Term, ent.Data)
	}
}

/*************************************
* leader election events logging.
**************************************/

func (logger *Logger) logElectionTimeout(serverId, currentTerm int, state StateType) {
	logger.printf(dElect, "S%v Timeout as %v at T:%v", serverId, state, currentTerm)
}

func (logger *Logger) logStartNewElection(serverId, currentTerm int, state StateType) {
	logger.printf(dElect, "S%v Start new election as %v at T:%v", serverId, state, currentTerm)
}

func (logger *Logger) logStepDown(serverId, oldTerm, newTerm int, stepDown bool) {
	if stepDown {
		logger.printf(dElect, "S%v Updated term and stepped down (T:%v -> T:%v)", serverId, oldTerm, newTerm)
	} else {
		logger.printf(dElect, "S%v Updated term but not stepped down (T:%v -> T:%v)", serverId, oldTerm, newTerm)
	}
}

func (logger *Logger) logBecomeLeader(serverId, currentTerm int) {
	logger.printf(dElect, "S%v Become leader at T:%v", serverId, currentTerm)
}

/*************************************
* voting events logging.
**************************************/

func (logger *Logger) logRequestVotes(serverId, currentTerm int) {
	logger.printf(dVote, "S%v Requesting votes at T:%v", serverId, currentTerm)
}

func (logger *Logger) logGrantVote(voteFromId, voteToId, voteFromTerm, voteToTerm int) {
	logger.printf(dVote, "S%v Granted vote to S%v (T:%v -> T:%v)", voteFromId, voteToId, voteFromTerm, voteToTerm)
}

type DenyVoteReason int

const (
	VOTH DenyVoteReason = iota // already voted to other server in this term.
	UPTD                       // this server has more up-to-date logs than the requesting server.
)

func (logger *Logger) logDenyVote(voteFromId, voteToId, voteFromTerm, voteToTerm int, reason DenyVoteReason) {
	logger.printf(dVote, "S%v Denied vote to S%v because %v (T:%v -> T:%v)", voteFromId, voteToId, reason, voteFromTerm, voteToTerm)
}

/*************************************
* client interaction events logging.
**************************************/

func (logger *Logger) logStartServer(serverId, currentTerm, logLength int) {
	logger.printf(dClient, "S%v Started (T:%v LN:%v)", serverId, currentTerm, logLength)
}

func (logger *Logger) logClientRequest(serverId, currentTerm int, isLeader bool, commandIndex int, command interface{}) {
	if isLeader {
		if showCommand {
			logger.printf(dClient, "S%v Accepted a client request at T:%v (CI:%v CMD:%v)", serverId, currentTerm, commandIndex, command)
		} else {
			logger.printf(dClient, "S%v Accepted a client request at T:%v (CI:%v)", serverId, currentTerm, commandIndex)
		}
	} else {
		logger.printf(dClient, "S%v Refused a client request at T:%v", serverId, currentTerm)
	}
}

func (logger *Logger) logApplyCommand(serverId, currentTerm int, state StateType, commandIndex int, command interface{}, lastApplied int) {
	if showCommand {
		logger.printf(dClient, "S%v Applied a command as %v at T:%v (CI:%v LA:%v CMD:%v)", serverId, state, currentTerm, commandIndex, lastApplied, command)
	} else {
		logger.printf(dClient, "S%v Applied a command as %v at T:%v (CI:%v LA:%v)", serverId, state, currentTerm, commandIndex, lastApplied)
	}
}

/*************************************
* log replication events logging.
**************************************/

func (logger *Logger) logAppendNewLog(serverId, currentTerm int, state StateType, beginIndex, newEntriesLength, logLength int) {
	logger.printf(dReplicate, "S%v Appended new logs as %v at T:%v (BLI:%v EN:%v LN:%v)", serverId, state, currentTerm, beginIndex, newEntriesLength, logLength)
}

func (logger *Logger) logDetectNewLog(leaderId, serverId, newEntriesLength int) {
	logger.printf(dReplicate, "S%v Detected %v new logs of S%v", leaderId, newEntriesLength, serverId)
}

func (logger *Logger) logSendNewLog(leaderID, serverId, currentTerm, prevLogIndex, prevLogTerm, newEntriesLength int) {
	logger.printf(dReplicate, "S%v Send new logs to S%v at T:%v (PLI:%v PLT:%v EN:%v)", leaderID, serverId, currentTerm, prevLogIndex, prevLogTerm, newEntriesLength)
}

type ConsistencyCheckResult int

const (
	PASS ConsistencyCheckResult = iota // pass the consistency check.
	NCTN                               // fail due to not contain a log with index args.PrevLogIndex.
	TMIS                               // fail due to term mismatch.
)

func (logger *Logger) logConsistencyCheck(serverId int, result ConsistencyCheckResult) {
	if showOnlyFailConsistencyCheck && result != PASS {
		logger.printf(dReplicate, "S%v Checked log consistency with result %v", serverId, result)
	} else if !showOnlyFailConsistencyCheck {
		logger.printf(dReplicate, "S%v Checked log consistency with result %v", serverId, result)
	}
}

func (logger *Logger) logHandleConflict(serverId, conflictIndex, deletedLogLength int) {
	if conflictIndex > 0 {
		logger.printf(dReplicate, "S%v Handled conflicts (LI:%v DN:%v)", serverId, conflictIndex, deletedLogLength)
	} else {
		logger.printf(dReplicate, "S%v Found no conflicts")
	}
}

func (logger *Logger) logUpdateNextIndex(leaderId, serverId, oldNextIndex, newNextIndex int) {
	logger.printf(dReplicate, "S%v Updated next index for S%v (NI:%v -> NI:%v)", leaderId, serverId, oldNextIndex, newNextIndex)
}

/*************************************
* log commit events logging.
**************************************/

func (logger *Logger) logSetCommit(serverId int, state StateType, oldCommitIndex, newCommitIndex int) {
	logger.printf(dCommit, "S%v Updated commit index as %v (CI:%v -> CI:%v)", serverId, state, oldCommitIndex, newCommitIndex)
}

func (logger *Logger) logNotifyMoreToApply(serverId, currentTerm, commitIndex, lastApplied int, state StateType) {
	logger.printf(dCommit, "S%v Notified there're more to apply as %v at T:%v (CI:%v LA:%v)", serverId, state, currentTerm, commitIndex, lastApplied)
}

/*************************************
* RPC communication events logging.
**************************************/

type RPCType int

const (
	HBET RPCType = iota // heartbeats.
	AENT                // AppendEntries RPC conveying log entries.
	RVOT                // RequestVote RPC.
)

func (logger *Logger) logSendMsg(sendFromId, sendToId, senderTerm int, state StateType, rpc RPCType) {
	if showRPCCommunication {
		logger.printf(dMsg, "S%v (T:%v %v) Send %v to S%v", sendFromId, senderTerm, state, rpc, sendToId)
	}
}

func (logger *Logger) logReceiveMsgRequest(receiverId, senderId, receiverTerm, senderTerm int, state StateType, rpc RPCType) {
	if showRPCCommunication {
		logger.printf(dMsg, "S%v (T:%v %v) Received %v request from S%v (T:%v)", receiverId, receiverTerm, state, rpc, senderId, senderTerm)
	}
}

func (logger *Logger) logReceiveMsgReply(receiverId, senderId, receiverTerm, senderTerm int, state StateType, rpc RPCType) {
	if showRPCCommunication {
		logger.printf(dMsg, "S%v (T:%v %v) Received %v reply from S%v (T:%v)", receiverId, receiverTerm, state, rpc, senderId, senderTerm)
	}
}

/*************************************
* reject RPC events logging.
**************************************/

// type RejectReason int

// const (
// 	STAL RejectReason = iota // the RPC is stale.
// 	TCHG                     // term has changed since sending the RPC.
// 	SCHG                     // state has changed since sending the RPC.
// )

// func (logger *Logger) logRejectRPCRequest(receiverId, senderId int, rpc RPCType, reason RejectReason) {
// 	logger.printf(dReject, "S%v Rejected %v request from S%v because %v", receiverId, rpc, senderId, reason)
// }

// func (logger *Logger) logRejectRPCResponse(receiverId, senderId int, rpc RPCType, reason RejectReason) {
// 	logger.printf(dReject, "S%v Rejected %v reply from S%v because %v", receiverId, rpc, senderId, reason)
// }

/*************************************
* reset timer events logging.
**************************************/

type ResetTimerReason int

const (
	LEAD ResetTimerReason = iota // receive an AppendEntries RPC request from the current leader.
	NELE                         // start a new election.
	GVOT                         // grant a vote to another peer.
)

func (logger *Logger) logResetTimer(serverId, currentTerm int, state StateType, reason ResetTimerReason) {
	if showResetTimer {
		logger.printf(dTimer, "S%v Reset timer as %v at T:%v because %v", serverId, state, currentTerm, reason)
	}
}

/*************************************
* persistence events logging.
**************************************/

// func (logger *Logger) logBackup(serverId, currentTerm, votedFor int, log []pb.Entry) {
// 	logger.printf(dPersist, "S%v Backed up (T:%v VF:%v LN:%v)", serverId, currentTerm, votedFor, len(log)-1)
// 	if showLog {
// 		for logIndex := 1; logIndex <= len(log)-1; logIndex++ {
// 			logger.printf(dPersist, "LOG: %v:%v", logIndex, log[logIndex].Command)
// 		}
// 	}
// }

// func (logger *Logger) logRestore(serverId, currentTerm, votedFor int, log []pb.Entry) {
// 	logger.printf(dPersist, "S%v Restored (T:%v VF:%v LN:%v)", serverId, currentTerm, votedFor, len(log)-1)
// 	if showLog {
// 		for logIndex := 1; logIndex <= len(log)-1; logIndex++ {
// 			logger.printf(dPersist, "LOG: %v:%v", logIndex, log[logIndex].Command)
// 		}
// 	}
// }

/*************************************
* snapshotting events logging.
**************************************/

func (logger *Logger) logReceiveSnapshotFromService(serverId, snapshotIndex int) {
	logger.printf(dSnap, "S%v received a snapshot from service (SI: %v)", serverId, snapshotIndex)
}

func (logger *Logger) logSnapshotAt(serverId, snapshotIndex int) {
	logger.printf(dSnap, "S%v snapshot at %v", serverId, snapshotIndex)
}

func (logger *Logger) logFindLaggedFollower(leaderId, followerId, leaderLastIncludedIndex, FollowerPrevLogIndex int) {
	logger.printf(dSnap, "S%v found S%v lags behind (LII: %v PLI: %v)", leaderId, followerId, leaderLastIncludedIndex, FollowerPrevLogIndex)
}

func (logger *Logger) logSendSnapshotToFollower(leaderId, followerId, snapshotIndex int) {
	logger.printf(dSnap, "S%v sent a snapshot to S%v (SI: %v)", leaderId, followerId, snapshotIndex)
}

func (logger *Logger) logFowardSnapshotToService(serverId, lastIncludedIndex, lastIncludedTerm int) {
	logger.printf(dSnap, "S%v forward a snapshot to service (SI: %v ST: %v)", serverId, lastIncludedIndex, lastIncludedTerm)
}

func (logger *Logger) logCondInstallSnapshot(serverId, lastIncludedIndex, lastIncludedTerm int) {
	logger.printf(dSnap, "S%v may install a snapshot (SI: %v ST: %v)", serverId, lastIncludedIndex, lastIncludedTerm)
}

/*************************************
* helpers for logging.
**************************************/

// not delete this for backward compatibility.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

// retrieve the verbosity level from an environment variable
// VERBOSE=0/1/2/3 <=>
func getVerbosityLevel() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

/*************************************
* stringers for logging.
**************************************/

// StateType stringer.
// func (state StateType) String() string {
// 	switch state {
// 	case Follower:
// 		return "Follower"
// 	case Candidate:
// 		return "Candidate"
// 	case Leader:
// 		return "Leader"
// 	default:
// 		log.Fatal("unreachable")
// 	}
// 	return ""
// }

// DenyVoteReason stringer.
func (reason DenyVoteReason) String() string {
	switch reason {
	case VOTH:
		return "voted to other"
	case UPTD:
		return "has more up-to-date log"
	default:
		log.Fatal("unreachable")
	}
	return ""
}

// ConsistencyCheckResult stringer.
func (result ConsistencyCheckResult) String() string {
	switch result {
	case PASS:
		return "PASS"
	case NCTN:
		return "FAIL (not contain a log at given index)"
	case TMIS:
		return "FAIL (term mismatch)"
	default:
		log.Fatal("unreachable")
	}
	return ""
}

// RPCType stringer.
func (rpc RPCType) String() string {
	switch rpc {
	case HBET:
		return "HBET"
	case AENT:
		return "AENT"
	case RVOT:
		return "RVOT"
	default:
		log.Fatal("unreachable")
	}
	return ""
}

// func (reason RejectReason) String() string {
// 	switch reason {
// 	case STAL:
// 		return "stale"
// 	case TCHG:
// 		return "term changed"
// 	case SCHG:
// 		return "state changed"
// 	default:
// 		log.Fatal("unreachable")
// 	}
// 	return ""
// }

func (reason ResetTimerReason) String() string {
	switch reason {
	case LEAD:
		return "contacted with current leader"
	case NELE:
		return "started a new election"
	case GVOT:
		return "granted a vote"
	default:
		log.Fatal("unreachable")
	}
	return ""
}
