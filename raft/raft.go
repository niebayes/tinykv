// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term when ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id    uint64
	peers []uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records:
	// if a node with id id has not votes for me in the current term yet, key id does not exist.
	// if a node with id id supports me to become the leader, votes[id] = true.
	// if a node with id id rejects me to become the leader, votes[id] = false.
	votes map[uint64]bool

	// msgs need to send.
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval
	heartbeatTimeout int
	// baseline of election interval.
	electionTimeoutBase int
	// the actual election interval.
	// electionTimeout = electionTimeoutBase + randomTimeout
	// where randomTimeout is chosed randomly in the range [electionTimeoutBase, 2*electionTimeoutBase].
	electionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout when it is leader or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	r := &Raft{
		id: c.ID,
		// FIXME: Shall I deep-copy the peers slice?
		peers:               c.peers,
		Term:                0,
		Vote:                None,
		RaftLog:             newLog(c.Storage),
		Prs:                 make(map[uint64]*Progress),
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		msgs:                make([]pb.Message, 0),
		Lead:                None,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		electionTimeoutBase: c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
	}

	return r
}

func (r *Raft) makeEntries() []*pb.Entry {
	entries := make([]*pb.Entry, 0)
	for _, entry := range r.RaftLog.entries {
		entries = append(entries, &entry)
	}
	return entries
}

func (r *Raft) makeAppendEntries(to uint64) pb.Message {
	entries := r.makeEntries()
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: entries,
	}
	return msg
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	msg := r.makeAppendEntries(to)
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) bcastAppendEntries() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

func (r *Raft) makeNoopEntry() []*pb.Entry {
	noop_entry := make([]*pb.Entry, 0)
	noop_entry = append(noop_entry, &pb.Entry{
		EntryType: pb.EntryType_EntryNoop,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      make([]byte, 0),
	})
	return noop_entry
}

func (r *Raft) makeAppendEntriesNoop(to uint64) pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: r.makeNoopEntry(),
	}
	return msg
}

func (r *Raft) sendAppendEntriesNoop(to uint64) {
	msg := r.makeAppendEntriesNoop(to)
	r.msgs = append(r.msgs, msg)
}

// upon becoming a new leader, broadcast a no-op entry to claim the leadership
// and keep other servers' log in sync.
func (r *Raft) bcastAppendEntriesNoop() {
	for _, to := range r.peers {
		// skip myself.
		if to != r.id {
			r.sendAppendEntriesNoop(to)
		}
	}
}

func (r *Raft) makeRequestVote(to uint64) pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	return msg
}

func (r *Raft) sendRequestVote(to uint64) {
	msg := r.makeRequestVote(to)
	r.msgs = append(r.msgs, msg)
}

// upon becoming a new candidate, broadcast a RequestVote to start a new round of election.
func (r *Raft) bcastRequestVote() {
	for _, to := range r.peers {
		// skip myself.
		if to != r.id {
			r.sendRequestVote(to)
		}
	}
}

func (r *Raft) makeHeartbeat(to uint64) pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		// heartbeat conveys empty entries.
		// the test suites require it to be nil rather than empty slice
		Entries: nil,
		Index:   0,
		LogTerm: 0,
	}
	return msg
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := r.makeHeartbeat(to)
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) bcastHeartbeat() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

// advance election timer and check if it times out. If times out, become candidate.
// only followers and candidates keep track of the election timer.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
    msg := pb.Message{
      MsgType: pb.MessageType_MsgHup,
    }
    r.Step(msg)
  }
}

// advance heartbeat timer and check if it times out. It times out, broadcast heartbeats.
// only leaders keep track of the heartbeat timer.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.bcastHeartbeat()
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	default:
		panic("invalid state")
	}
}

// becomeFollower transform this peer's state to Follower
// @param term
// @param lead the node with id lead is believed to be the current leader.
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead

	r.votes = make(map[uint64]bool)
	r.Vote = None
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	// raft introduces randomization into election timer to resolve split vote faster.
	r.electionTimeout = r.electionTimeoutBase + (rand.Int() % r.electionTimeoutBase)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State != StateFollower && r.State != StateCandidate {
		panic("invalid state transition")
	}
	// increment term.
	r.Term++

	// vote for self.
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

	// reset election timer.
	r.resetElectionTimer()

	// now it's safe to become a candidate.
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// reset vote record since leader does not need them.
	r.Vote = None
	r.votes = make(map[uint64]bool)

	r.State = StateLeader
}

func (r *Raft) stepFollower(msg pb.Message) {
	switch msg.MsgType {
  case pb.MessageType_MsgHup:
    r.handleMsgHup(msg)
	case pb.MessageType_MsgPropose:
		r.appendEntries(msg.Entries)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(msg)
	case pb.MessageType_MsgRequestVoteResponse:
		// dropped.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgHeartbeatResponse:
		// dropped.
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgAppendResponse:
		// dropped.
	default:
		panic("invalid msg type")
	}
}

func (r *Raft) stepCandidate(msg pb.Message) {
	switch msg.MsgType {
  case pb.MessageType_MsgHup:
    r.handleMsgHup(msg)
	case pb.MessageType_MsgPropose:
		// dropped.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(msg)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(msg)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgHeartbeatResponse:
		// dropped.
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgAppendResponse:
		// dropped.
	default:
		panic("invalid msg type")
	}
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
}

func (r *Raft) stepLeader(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgPropose:
		r.appendEntries(msg.Entries)
		r.bcastAppendEntries()
	case pb.MessageType_MsgRequestVote:
		// dropped.
	case pb.MessageType_MsgRequestVoteResponse:
		// dropped.
	case pb.MessageType_MsgHeartbeat:
		// dropped.
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(msg)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(msg)
	default:
		panic("invalid msg type")
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(msg pb.Message) error {
	// TODO:
	// Note that every step is checked by one common method
	// 'Step' that safety-checks the terms of node and incoming message to prevent
	// stale log entries:

	// FIXME: Shall I differentiate between local messages and network messages?
	switch r.State {
	case StateFollower:
		r.stepFollower(msg)
	case StateCandidate:
		r.stepCandidate(msg)
	case StateLeader:
		r.stepLeader(msg)
	default:
		panic("invalid state")
	}
	return nil
}

// handle MsgHup message
func (r *Raft) handleMsgHup(msg pb.Message) {
  r.becomeCandidate()
  r.bcastRequestVote()
}

// handle RequestVote RPC request.
func (r *Raft) handleRequestVote(req pb.Message) {
	// if I'm stale, keep pace with the candidate.
	if req.Term > r.Term {
		// update my term, since
		r.becomeFollower(req.Term, req.From)
	}

	reject := true // reject granting the vote?
	// grant vote only if he is not stale.
	// FIXME: Shall check state? For e.g. only candidate or follower can grant vote?
	if req.Term >= r.Term {
		// r.Vote == req.From says: since the network may duplicate the RPC request,
		// I have to vote for the same candidate once again if necessary.
		if r.Vote == None || r.Vote == req.From {
			r.Vote = req.From
			reject = false
		}
	}

	// reset election timer since I've granted the vote and this may result in spawning a new leader.
	// I don't want to compete with the new leader.
	if !reject {
		r.resetElectionTimer()
	}

	// construct RequestVote response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      req.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	// send the response.
	r.msgs = append(r.msgs, res)
}

// handle RequestVote RPC response.
func (r *Raft) handleRequestVoteResponse(res pb.Message) {
	if res.Term > r.Term {
		r.becomeFollower(res.Term, res.From)
		// stop voting if I step down.
		return
	}

	if r.State != StateCandidate {
		// stop voting if I'm not a candidate any more. I.e. I'm the leader and this response is stale.
		return
	}

	// whatever the from node voted for or against, record it.
	r.votes[res.From] = res.Reject == true

	num_supports := 0 // number of nodes supporting me to become the leader.
	num_denials := 0  // number of nodes rejecting me to become the leader.
	for _, support := range r.votes {
		if support {
			num_supports++
		} else {
			num_denials++
		}
	}

	// a majority of nodes in the cluster support me, I become the new leader.
	if 2*num_supports > len(r.peers) {
		r.becomeLeader()
		// upon becoming a new leader, broadcast a no-op entry to claim the leadership
		// and keep other nodes' log in sync.
		r.bcastAppendEntriesNoop()
		return
	}

	// a majority of nodes in the cluster reject me, I step down.
	if 2*num_denials > len(r.peers) {
		r.becomeFollower(r.Term, res.From)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(req pb.Message) {
	// step down if I'm stale.
	if req.Term > r.Term {
		r.becomeFollower(req.Term, req.From)
	}

	// reject if the from node is stale.
	reject := false
	if req.Term < r.Term {
		reject = true
	}

	// FIXME: Is this strategy correct?
	if !reject {
		// I admit the from node is the current leader, so I step down and reset election timer
		// to not compete with him.
		r.becomeFollower(req.Term, req.From)
		r.resetElectionTimer()
	}

	// construct the response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      req.From,
		From:    r.id,
		Reject:  reject,
	}

	// send the response.
	r.msgs = append(r.msgs, res)
}

// handle AppendEntries RPC response.
func (r *Raft) handleAppendEntriesResponse(res pb.Message) {

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(req pb.Message) {
	// step down if I'm stale.
	if req.Term > r.Term {
		r.becomeFollower(req.Term, req.From)
	}

	// reject if the from node is stale.
	reject := false
	if req.Term < r.Term {
		reject = true
	}

	if !reject {
		// I admit the from node is the current leader, so I step down and reset election timer
		// to not compete with him.
		r.becomeFollower(req.Term, req.From)
		r.resetElectionTimer()
	}

	// construct the response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      req.From,
		From:    r.id,
		Reject:  reject,
	}

	// send the response.
	r.msgs = append(r.msgs, res)
}

// handle Heartbeat RPC response.
func (r *Raft) handleHeartbeatResponse(res pb.Message) {

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(msg pb.Message) {
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
