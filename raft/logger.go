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
const LOGTOFILE = false

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
		prefix := fmt.Sprintf("%09d %v ", time, string(topic))
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
// persistence events.
//

func (l *Logger) startRaft() {
	r := l.r
	l.printf(dPersist, "N%v START (T:%v V:%v CI:%v AI:%v SI:%v)", r.id, r.Term, r.Vote, r.RaftLog.committed,
		r.RaftLog.applied, r.RaftLog.stabled)
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
	l.printf(dElect, "N%v @ RVOT (T:%v)", r.id, r.Term)
}

func (l *Logger) recvRVOT(m pb.Message) {
	r := l.r
	l.printf(dElect, "N%v <- N%v RVOT (T:%v)", r.id, m.From, m.Term)
}

func (l *Logger) recvRVOTRes(m pb.Message) {
	r := l.r
	l.printf(dElect, "N%v <- N%v RVOT RES (T:%v R:%v)", r.id, m.From, m.Term, m.Reject)
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
	l.printf(dElect, "N%v %v->%v (T:%v) -> (T:%v)", r.id, r.State, StateFollower, oldTerm, r.Term)
}

//
// log replication events.
//

func (l *Logger) recvPROP(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- PROP (LN:%v)", r.id, len(m.Entries))
	for _, ent := range m.Entries {
		l.printf(dReplicate, "N%v\t (D:%v)", r.id, string(ent.Data))
	}
}

func (l *Logger) appendEnts(ents []pb.Entry) {
	r := l.r
	l.printf(dReplicate, "N%v +e (LN:%v)", r.id, len(ents))
	l.printEnts(dReplicate, r.id, ents)
}

func (l *Logger) bcastAENT() {
	r := l.r
	l.printf(dReplicate, "N%v @ AENT", r.id)
}

func (l *Logger) sendEnts(prevLogIndex, prevLogTerm uint64, ents []pb.Entry, to uint64) {
	r := l.r
	l.printf(dReplicate, "N%v e-> N%v (T:%v CI:%v PI:%v PT:%v LN:%v)", r.id, to, r.Term, r.RaftLog.committed, prevLogIndex, prevLogTerm, len(ents))
	l.printEnts(dReplicate, r.id, ents)
}

func (l *Logger) acceptEnts(from uint64) {
	r := l.r
	l.printf(dReplicate, "N%v &e<- N%v", r.id, from)
}

var reasonMap = [...]string{
	"NO", // not reject
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
	l.printEnts(dReplicate, r.id, ents)
}

func (l *Logger) updateProgOf(id, oldNext, oldMatch, newNext, newMatch uint64) {
	r := l.r
	l.printf(dReplicate, "N%v ^pr N%v (NI:%v MI:%v) -> (NI:%v MI:%v)", r.id, id, oldNext, oldMatch, newNext, newMatch)
}

func (l *Logger) updateCommitted(oldCommitted uint64) {
	r := l.r
	l.printf(dReplicate, "N%v ^ci (CI:%v) -> (CI:%v)", r.id, oldCommitted, r.RaftLog.committed)
}

func (l *Logger) updateStabled(oldStabled uint64) {
	r := l.r
	l.printf(dReplicate, "N%v ^si (SI:%v) -> (SI:%v)", r.id, oldStabled, r.RaftLog.stabled)
}

func (l *Logger) updateApplied(oldApplied uint64) {
	r := l.r
	l.printf(dReplicate, "N%v ^ai (AI:%v) -> (AI:%v)", r.id, oldApplied, r.RaftLog.applied)
}

func (l *Logger) recvAENT(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- N%v AENT (T:%v CI:%v PI:%v PT:%v LN:%v)", r.id, m.From, m.Term, m.Commit, m.Index, m.LogTerm, len(m.Entries))
}

func (l *Logger) recvAENTRes(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- N%v AENT RES (T:%v R:%v RR:%v NI:%v CT:%v)", r.id, m.From, m.Term, m.Reject, m.Reason, m.NextIndex, m.ConflictTerm)
}

//
// heartbeat events.
//

func (l *Logger) recvBEAT() {
	r := l.r
	l.printf(dReplicate, "N%v <- BEAT", r.id)
}

func (l *Logger) recvHBET(m pb.Message) {
	r := l.r
	l.printf(dReplicate, "N%v <- BEAT (T:%v CI:%v)", r.id, m.Term, m.Commit)
}

func (l *Logger) bcastHBET() {
	r := l.r
	l.printf(dReplicate, "N%v @ HBET", r.id)
}

func (l *Logger) beatTimeout() {
	r := l.r
	l.printf(dReplicate, "N%v BTO (S:%v T:%v)", r.id, r.State, r.Term)
}

//
// persistence event logging.
//

func (l *Logger) restoreEnts(ents []pb.Entry) {
	r := l.r
	l.printf(dPersist, "N%v RESTORE ENTS (LN:%v)", r.id, len(ents))
	l.printEnts(dPersist, r.id, ents)
}

func (l *Logger) printEnts(topic logTopic, id uint64, ents []pb.Entry) {
	for _, ent := range ents {
		l.printf(topic, "N%v    (I:%v T:%v D:%v)", id, ent.Index, ent.Term, string(ent.Data))
	}
}
