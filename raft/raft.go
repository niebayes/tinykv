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
	"F", // follower
	"C", // candidate
	"L", // leader
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// match index, the index of the highest log entry known to be replicated on server.
	// init to 0 and increases monotonically.
	Match uint64
	// next index, the index of the next log entry to send to that server.
	// init to leader last log index + 1.
	Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// the reason why use a map for Prs is that some nodes may go down and when leader awares of it,
	// the corresponding item in the Prs map can be removed, so that some delayed RPCs can be dropped.
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

	// the raft paper suggests to resume the client operations if leadership transfer does
	// not complete after about an election timeout.
	// we convervatively set the transferTimeout to 2*electionTimeoutBase which is the max
	// value an election timeout could be.
	transferTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout when it is leader or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last transferTimeout.
	transferElapsed int

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

	raftInitLogIndex uint64

	// TODO: use the global logger.
	Logger *Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	r := &Raft{
		id:                  c.ID,
		Term:                0,
		Vote:                None,
		RaftLog:             newLog(c.Storage),
		Prs:                 make(map[uint64]*Progress),
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		msgs:                make([]pb.Message, 0),
		Lead:                None,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeoutBase: c.ElectionTick,
		transferTimeout:     2 * c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
		transferElapsed:     0,
		raftInitLogIndex:    0,
		leadTransferee:      None,
	}

	// init logger.
	r.Logger = makeLogger(true, "raft.log")
	r.Logger.r = r

	// init peer progress. (only for testing).
	// TODO: add a hint for how to init peer progress in project2b.
	for _, id := range c.peers {
		r.Prs[id] = r.newProgress()
	}

	// check if there're some restored stable entries.
	l := r.RaftLog
	if l.stabled != 0 && l.Len() > 1 {
		r.Logger.restoreEnts(l.allEntries())
	}

	// restore persisted states.
	hardstate, confState, _ := c.Storage.InitialState()
	if !IsEmptyHardState(hardstate) {
		r.raftInitLogIndex = hardstate.Commit
		r.restoreHardState(&hardstate)
	}
	if len(confState.Nodes) > 0 {
		r.restoreConfState(&confState)
	}

	// on recovery, c.Applied is set according to persisted applied index.
	l.applied = max(c.Applied, l.applied)

	// TODO: restore persisted log from snapshot.

	r.resetElectionTimer()

	r.Logger.startRaft()

	return r
}

//
// tick: how raft module interacts with clock.
// Step: how raft module interacts with inbound messages.
// forwardMsgUp: how raft module interacts with outbound messages.
//

//
// tick simulates how the internal logical clock drives the raft's behavior over time.
//

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
		r.tickTransfer()
	default:
		panic("invalid state")
	}
}

// advance election timer and check if it times out. If times out, become candidate.
// only followers and candidates keep track of the election timer.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})

		r.Logger.elecTimeout()
	}
}

// advance heartbeat timer and check if it times out. It times out, broadcast heartbeats.
// only leaders keep track of the heartbeat timer.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      r.id,
		})

		r.Logger.beatTimeout()
	}
}

// advance transfer timer and check if it times out. If times out, abort the pending leader transfering
// and resume client operations.
func (r *Raft) tickTransfer() {
	// only tick if there's a pending leadership transferring.
	if r.leadTransferee == None {
		return
	}

	r.transferElapsed++
	if r.transferElapsed >= r.transferTimeout {
		r.transferElapsed = 0
		r.leadTransferee = None

		r.Logger.transTimeout()
	}
}

//
// Step is the entrance of message handling including local messages and network messages.
//

func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	default:
		panic("invalid state")
	}
	return nil
}

// how followers handle each type of msg.
func (r *Raft) stepFollower(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgBeat:
		// dropped.
	case pb.MessageType_MsgPropose:
		// dropped.
		// FIXME: Shall I redirect the proposal to the leader.
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
	case pb.MessageType_MsgSnapshot:
		r.handleInstallSnapshot(msg)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(msg)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(msg)
	default:
		panic("invalid msg type")
	}
}

// how candidates handle each type of msg.
func (r *Raft) stepCandidate(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgBeat:
		// dropped.
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
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(msg)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(msg)
	case pb.MessageType_MsgTimeoutNow:
		// FIXME: in etcd, candidates drops TransferLeader and TimeoutNow. Shall I also drop them?
		r.handleTimeoutNow(msg)
	default:
		panic("invalid msg type")
	}
}

// how leaders handle each type of msg.
func (r *Raft) stepLeader(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		// dropped.
	case pb.MessageType_MsgBeat:
		r.handleBeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(msg)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(msg)
	case pb.MessageType_MsgRequestVoteResponse:
		// dropped.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(msg)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(msg)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(msg)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(msg)
	case pb.MessageType_MsgTimeoutNow:
		// FIXME: Shall a leader handles TimeoutNow?
		r.handleTimeoutNow(msg)
	default:
		panic("invalid msg type")
	}
}

//
// forwardMsgUp through which the raft module interacts with the upper application.
//

// forward the message msg to the upper application which is responsible for sending out the message.
func (r *Raft) forwardMsgUp(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) GetMostUpToDatePeerId() uint64 {
	l := r.RaftLog
	mostUpToDatePeerId := None
	minMatchDiff := l.LastIndex() + 1
	for id, pr := range r.Prs {
		if id == r.id {
			continue
		}
		matchDiff := l.LastIndex() - pr.Match
		if matchDiff < minMatchDiff {
			minMatchDiff = matchDiff
			mostUpToDatePeerId = id
		}
	}
	return mostUpToDatePeerId
}
