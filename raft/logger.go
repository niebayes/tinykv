package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	// "github.com/pingcap-incubator/tinykv/kv/raftstore/peer"
	"github.com/pingcap-incubator/tinykv"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

// true to turn on debugging/logging.
const debug = tinykv.DEBUG
const LOGTOFILE = false

// what topic the log message is related to.
// logs are organized by topics which further consists of events.
type logTopic string

const (
	// the typical route of leader election is:
	// 	becomeFollower
	//		election time out
	//		send MsgHup to self
	//	becomeCandidate
	//  bcastRequestVote
	//		other peers: handleRequestVote
	//			grant vote
	//			deny vote
	//	handleRequestVoteResponse
	//		receive a majority of votes
	// 	becomeLeader
	//		append a noop entry
	//		bcast the noop entry
	ELEC logTopic = "ELEC"

	// the typical route of log replication is:
	//	receive MsgProp
	//		append these log entries
	//		update leader's own progress
	//	bcastAppendEntries
	//		other peers: handleAppendEntries
	//			reject the whole entries due to index conflict or term conflict
	//			accept the whole entries but discard conflicting entries and only append missing entries.
	//	handleAppendEntriesResponse
	//		leader update follower's progress: next index and match index
	//		leader knows which entries are committed
	//	bcastHeartbeat
	//		other peers know which entries are committed
	// 	handleHeartbeatResponse
	//		leader notifys slow followers and send AppendEntries to make them catch up.
	//		...
	//		all alive followers commit the log entries
	//
	LRPE logTopic = "LRPE"

	// heartbeat events:
	// leader heartbeat time out
	// leader broadcast HeartBeat
	// others receive HeartBeat
	// leader receive HeartBeatResponse
	BEAT logTopic = "BEAT"

	// persistence events:
	// restore stable entries from stable storage.
	// restore term, vote, commit from hardstate.
	// restore nodes config from confstate.
	// persist unstable log entrie.
	// update and save hardstate
	// update and save applystate.
	PERS logTopic = "PERS"

	// peer handling events:
	//	start raft module
	//  propose new raft cmd
	//  detect ready raft states.
	//  notify clients stale proposals.
	//  process committed log entry/raft cmd
	//  advance raft state
	PEER logTopic = "PEER"

	// snapshotting events:
	// service sends a snapshot
	// server snapshots
	// leader detects a follower is lagging hebind
	// leader sends InstallSnapshot to lagged follower
	// follower forwards snapshot to service
	// service conditionally installs a snapshot by asking Raft.
	SNAP logTopic = "SNAP"
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
	logger.init(LOGTOFILE, logFileName)
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
		// time := time.Since(logger.startTime).Milliseconds()
		time := time.Since(logger.startTime).Microseconds()
		// e.g. 008256 VOTE ...
		prefix := fmt.Sprintf("%010d %v ", time, string(topic))
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

//
// leader election events.
//

func (l *Logger) recvHUP() {
	r := l.r
	l.printf(ELEC, "N%v <- HUP (T:%v)", r.id, r.Term)
}

func (l *Logger) elecTimeout() {
	r := l.r
	l.printf(ELEC, "N%v ETO (S:%v T:%v)", r.id, r.State, r.Term)
}

func (l *Logger) stateToCandidate() {
	r := l.r
	l.printf(ELEC, "N%v %v->%v (T:%v)", r.id, r.State, StateCandidate, r.Term)
}

func (l *Logger) bcastRVOT() {
	r := l.r
	l.printf(ELEC, "N%v @ RVOT (T:%v)", r.id, r.Term)
}

func (l *Logger) recvRVOT(m pb.Message) {
	r := l.r
	l.printf(ELEC, "N%v <- N%v RVOT (T:%v)", r.id, m.From, m.Term)
}

func (l *Logger) voteTo(to uint64) {
	r := l.r
	l.printf(ELEC, "N%v v-> N%v", r.id, to)
}

var denyReasonMap = [...]string{
	"GRT", // grant the vote.
	"VTD", // I've granted the vote to another one.
	"STL", // you're stale.
}

func (l *Logger) rejectVoteTo(to uint64, reason pb.DenyVoteReason, CandidatelastLogIndex, CandidatelastLogTerm, lastLogIndex, lastLogTerm uint64) {
	r := l.r
	l.printf(ELEC, "N%v !v-> N%v COZ %v (CLI:%v CLT:%v LI:%v LT:%v)", r.id, to,
		denyReasonMap[reason], CandidatelastLogIndex, CandidatelastLogTerm, lastLogIndex, lastLogTerm)
}

func (l *Logger) recvRVOTRes(m pb.Message) {
	r := l.r
	l.printf(ELEC, "N%v <- N%v RVOT RES (T:%v R:%v)", r.id, m.From, m.Term, m.Reject)
}

func (l *Logger) recvVoteQuorum(num_supports uint64) {
	r := l.r
	l.printf(ELEC, "N%v <- VOTE QUORUM (T:%v NS:%v NN:%v)", r.id, r.Term, num_supports, len(r.Prs))
}

func (l *Logger) recvDenyQuorum(num_denials uint64) {
	r := l.r
	l.printf(ELEC, "N%v <- DENY QUORUM (T:%v ND:%v NN:%v)", r.id, r.Term, num_denials, len(r.Prs))
}

func (l *Logger) stateToLeader() {
	r := l.r
	l.printf(ELEC, "N%v %v->%v (T:%v)", r.id, r.State, StateLeader, r.Term)
}

func (l *Logger) stateToFollower(oldTerm uint64) {
	r := l.r
	l.printf(ELEC, "N%v %v->%v (T:%v) -> (T:%v)", r.id, r.State, StateFollower, oldTerm, r.Term)
}

//
// log replication events.
//

func (l *Logger) recvPROP(m pb.Message) {
	r := l.r
	l.printf(LRPE, "N%v <- PROP (LN:%v)", r.id, len(m.Entries))
}

func (l *Logger) appendEnts(ents []pb.Entry) {
	r := l.r
	l.printf(LRPE, "N%v +e (LN:%v)", r.id, len(ents))
	// l.printEnts(LRPE, r.id, ents)
}

func (l *Logger) bcastAENT() {
	r := l.r
	l.printf(LRPE, "N%v @ AENT", r.id)
}

func (l *Logger) sendEnts(prevLogIndex, prevLogTerm uint64, ents []pb.Entry, to uint64) {
	r := l.r
	l.printf(LRPE, "N%v e-> N%v (T:%v CI:%v PI:%v PT:%v LN:%v)", r.id, to, r.Term, r.RaftLog.committed, prevLogIndex, prevLogTerm, len(ents))
	// l.printEnts(LRPE, r.id, ents)
}

func (l *Logger) recvAENT(m pb.Message) {
	r := l.r
	l.printf(LRPE, "N%v <- N%v AENT (T:%v CI:%v PI:%v PT:%v LN:%v)", r.id, m.From, m.Term, m.Commit, m.Index, m.LogTerm, len(m.Entries))
}

var reasonMap = [...]string{
	"NO", // not reject
	"IC", // index conflict.
	"TC", // term conflict.
}

func (l *Logger) rejectEnts(reason pb.RejectReason, from, prevLogIndex, prevLogTerm uint64) {
	r := l.r
	if reason == pb.RejectReason_IndexConflict {
		l.printf(LRPE, "N%v !e<- N%v COZ %v (PI:%v LI:%v)", r.id, from, reasonMap[reason], prevLogIndex, r.RaftLog.LastIndex())
	} else {
		term, _ := r.RaftLog.Term(prevLogIndex)
		l.printf(LRPE, "N%v !e<- N%v COZ %v (PT:%v T:%v)", r.id, from, reasonMap[reason], prevLogTerm, term)
	}
}

func (l *Logger) acceptEnts(from uint64) {
	r := l.r
	l.printf(LRPE, "N%v &e<- N%v", r.id, from)
}

func (l *Logger) discardEnts(ents []pb.Entry) {
	r := l.r
	l.printf(LRPE, "N%v -e (LN:%v)", r.id, len(ents))
	l.printEnts(LRPE, r.id, ents)
}

func (l *Logger) recvAENTRes(m pb.Message) {
	r := l.r
	l.printf(LRPE, "N%v <- N%v AENT RES (T:%v R:%v RR:%v NI:%v CT:%v)", r.id, m.From, m.Term, m.Reject, reasonMap[m.Reason], m.NextIndex, m.ConflictTerm)
}

func (l *Logger) updateProgOf(id, oldNext, oldMatch, newNext, newMatch uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^pr N%v (NI:%v MI:%v) -> (NI:%v MI:%v)", r.id, id, oldNext, oldMatch, newNext, newMatch)
}

func (l *Logger) recvAppendQuorum(cnt int) {
	r := l.r
	l.printf(ELEC, "N%v <- APED QUORUM (NA:%v NN:%v)", r.id, cnt, len(r.Prs))
}

func (l *Logger) updateCommitted(oldCommitted uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^ci (CI:%v) -> (CI:%v)", r.id, oldCommitted, r.RaftLog.committed)
}

func (l *Logger) updateStabled(oldStabled uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^si (SI:%v) -> (SI:%v)", r.id, oldStabled, r.RaftLog.stabled)
}

func (l *Logger) updateApplied(oldApplied uint64) {
	r := l.r
	l.printf(LRPE, "N%v ^ai (AI:%v) -> (AI:%v)", r.id, oldApplied, r.RaftLog.applied)
}

func (l *Logger) printEnts(topic logTopic, id uint64, ents []pb.Entry) {
	for _, ent := range ents {
		// l.printf(topic, "N%v    (I:%v T:%v D:%v)", id, ent.Index, ent.Term, string(ent.Data))
		l.printf(topic, "N%v    (I:%v T:%v)", id, ent.Index, ent.Term)
	}
}

//
// heartbeat events.
//

func (l *Logger) beatTimeout() {
	r := l.r
	l.printf(BEAT, "N%v BTO (S:%v T:%v)", r.id, r.State, r.Term)
}

func (l *Logger) recvBEAT() {
	r := l.r
	l.printf(BEAT, "N%v <- BEAT", r.id)
}

func (l *Logger) bcastHBET() {
	r := l.r
	l.printf(BEAT, "N%v @ HBET", r.id)
}

func (l *Logger) recvHBET(m pb.Message) {
	r := l.r
	l.printf(BEAT, "N%v <- N%v HBET (T:%v CI:%v)", r.id, m.From, m.Term, m.Commit)
}

//
// persistence events.
//

func (l *Logger) restoreEnts(ents []pb.Entry) {
	r := l.r
	l.printf(PERS, "N%v re (LN:%v)", r.id, len(ents))
	l.printEnts(PERS, r.id, ents)
}

func (l *Logger) restoreHardState(hardState *pb.HardState) {
	r := l.r
	l.printf(PERS, "N%v +hs (T:%v V:%v CI:%v)", r.id, hardState.Term, hardState.Vote, hardState.Commit)
}

func (l *Logger) restoreConfState(confState *pb.ConfState) {
	r := l.r
	l.printf(PERS, "N%v +cs (NN:%v)", r.id, len(confState.Nodes))
}

func (l *Logger) PersistEnts(oldlastStabledIndex, lastStabledIndex uint64) {
	r := l.r
	// be: backup entries.
	l.printf(PERS, "N%v be (SI:%v) -> (SI:%v)", r.id, oldlastStabledIndex, lastStabledIndex)
}

func (l *Logger) UpdateHardState(prevHardState pb.HardState) {
	r := l.r
	curHardState := r.hardState()
	l.printf(PERS, "N%v ^hs (T:%v V:%v CI:%v) -> (T:%v V:%v CI:%v)", r.id, prevHardState.Term,
		prevHardState.Vote, prevHardState.Commit, curHardState.Term, curHardState.Vote, curHardState.Commit)
}

func (l *Logger) UpdateApplyState(oldAppliedIndex, newAppliedIndex uint64) {
	r := l.r
	l.printf(PERS, "N%v ^as (AI:%v) -> (AI:%v)", r.id, oldAppliedIndex, newAppliedIndex)
}

//
// peer interaction events.
//

func (l *Logger) startRaft() {
	r := l.r
	l.printf(PEER, "N%v START (T:%v V:%v CI:%v AI:%v SI:%v)", r.id, r.Term, r.Vote, r.RaftLog.committed,
		r.RaftLog.applied, r.RaftLog.stabled)
}

func (l *Logger) NewProposal(m *raft_cmdpb.RaftCmdRequest, propIndex, propTerm uint64) {
	r := l.r
	for _, request := range m.Requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			l.printf(PEER, "N%v <- GET (K:%v) (I:%v T:%v)", r.id, string(request.Get.Key), propIndex, propTerm)

		case raft_cmdpb.CmdType_Put:
			l.printf(PEER, "N%v <- PUT (K:%v V:%v) (I:%v T:%v)", r.id, string(request.Put.Key), string(request.Put.Value), propIndex, propTerm)

		case raft_cmdpb.CmdType_Delete:
			l.printf(PEER, "N%v <- DEL (K:%v) (I:%v T:%v)", r.id, string(request.Delete.Key), propIndex, propTerm)

		case raft_cmdpb.CmdType_Snap:
			l.printf(PEER, "N%v <- SNP (I:%v T:%v)", r.id, propIndex, propTerm)

		default:
			panic("unknown cmd type")
		}
	}
}

func (l *Logger) ReadyCommittedEnts(ents []pb.Entry) {
	r := l.r
	l.printf(PEER, "N%v RD CMT ENTS (LN:%v)", r.id, len(ents))
	l.printEnts(PEER, r.id, ents)
}

func (l *Logger) NotifyStaleProp(propIndex, propTerm, entIndex, entTerm uint64) {
	r := l.r
	l.printf(PEER, "N%v STALE PROP (PI:%v PT:%v I%v: T:%v)", r.id, propIndex, propTerm, entIndex, entTerm)
}

func (l *Logger) NotifyClient(propIndex uint64) {
	r := l.r
	l.printf(PEER, "N%v NOTIFY PROP %v", r.id, propIndex)
}

func (l *Logger) ProcessedPropNoop(propIndex uint64) {
	r := l.r
	l.printf(PEER, "N%v PROC PROP NOOP %v", r.id, propIndex)
}

func (l *Logger) ProcessedProp(propIndex uint64) {
	r := l.r
	l.printf(PEER, "N%v PROC PROP %v", r.id, propIndex)
}

func (l *Logger) Advance() {
	r := l.r
	l.printf(PEER, "N%v ADV", r.id)
}

func (l *Logger) AdvanceRaft(oldStabled, oldApplied, oldLastIncludedIndex,
	oldLastIncludedTerm, prevTerm, prevVote, prevCommit uint64) {
	r := l.r
	hardState := r.hardState()
	l.printf(PEER, "N%v ADV RAFT (SI:%v AI:%v LI:%v LT:%v HT:%v HV:%v HC:%v) -> (SI:%v AI:%v LI:%v LT:%v HT:%v HV:%v HC:%v)",
		r.id, oldStabled, oldApplied, oldLastIncludedIndex, oldLastIncludedTerm, prevTerm, prevVote, prevCommit,
		r.RaftLog.stabled, r.RaftLog.applied, r.RaftLog.lastIncludedIndex, r.RaftLog.lastIncludedTerm,
		hardState.Term, hardState.Vote, hardState.Commit)
}

// func (l *Logger) UpdateProposals(oldProposals, newProposals []*peer.proposal) {
// }
