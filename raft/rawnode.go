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

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// FIXME: the SoftState seems never could be nil?
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// I.e. Entries = unstable entries.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be applied to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// note, committed entries do not contain applied entries.
	// i.e. committed entries = entries[applied+1 : committed+1]
	// note, slicing in Go is left closed and right open.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft          *Raft
	prevHardState pb.HardState
	prevSoftState *SoftState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(cfg *Config) (*RawNode, error) {
	r := newRaft(cfg)
	rn := &RawNode{
		Raft:          r,
		prevHardState: r.hardState(),
		prevSoftState: r.softState(),
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	rd := Ready{}
	l := rn.Raft.RaftLog
	rd.Entries = l.unstableEntries()
	rd.CommittedEntries = l.nextEnts()
	rd.Messages = rn.Raft.msgs
	// be sure to empty the mailbox after retrieving.
	// FIXME: Shall I empty the mailbox right here, or in Advance after the ready state are processed?
	rn.Raft.msgs = rn.Raft.msgs[:0]
	// to fit into the creepy tests.
	if len(rd.Messages) == 0 {
		rd.Messages = nil
	}
	if curHardState := rn.Raft.hardState(); !isHardStateEqual(curHardState, rn.prevHardState) {
		rd.HardState = curHardState
	}
	if l.hasPendingSnapshot() {
		// FIXME: Shall nullify pendingSnapshot right now?
		rd.Snapshot = *l.pendingSnapshot
	}

	if curSoftState := rn.Raft.softState(); !curSoftState.equal(rn.prevSoftState) {
		rd.SoftState = curSoftState
	}

	// rn.Raft.Logger.ReadyCommittedEnts(rd.CommittedEntries)

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
// (1) if there're msgs to be sent.
// (2) if there're newly appended entries to be stabled.
// (3) if there're newly committed entries to be applied,
// (4) if there're pending snapshot to be installed.
// (5) if the hardstate needs to be updated.
func (rn *RawNode) HasReady() bool {
	l := rn.Raft.RaftLog
	curSoftState := rn.Raft.softState()
	curHardState := rn.Raft.hardState()
	updatedHardState := !IsEmptyHardState(curHardState) && !isHardStateEqual(curHardState, rn.prevHardState)
	return len(rn.Raft.msgs) > 0 || len(l.unstableEntries()) > 0 ||
		len(l.nextEnts()) > 0 || l.hasPendingSnapshot() ||
		updatedHardState || !curSoftState.equal(rn.prevSoftState)
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// try to advance applied index if there're committed entries (not yet applied) ready to be applied.
// try to advance stabled index if there're unstable entries to be persisted.
// try to install a new snapshot if there's one pending snapshot.
func (rn *RawNode) Advance(rd Ready) {
	// do not logging if there're only msgs to be sent.
	if len(rd.Entries) > 0 || len(rd.CommittedEntries) > 0 ||
		!IsEmptySnap(&rd.Snapshot) || !IsEmptyHardState(rd.HardState) {
		rn.Raft.Logger.Advance()
	}

	l := rn.Raft.RaftLog
	// oldStabled := l.stabled
	// oldApplied := l.applied
	// oldLastIncludedIndex := l.lastIncludedIndex
	// oldLastIncludedTerm := l.lastIncludedTerm
	// prevTerm := rn.prevHardState.Term
	// prevVote := rn.prevHardState.Vote
	// prevCommit := rn.prevHardState.Commit

	// unstable entries were persisted to the stable storage.
	if len(rd.Entries) > 0 {
		rn.Raft.tryUpdateStabled(rd.Entries[len(rd.Entries)-1].Index)
	}
	// committed entries were applied to the state machine.
	// note, no need to check rd.CommittedEntries != nil, since len(nil) returns 0.
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.tryUpdateApplied(rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		// notify the raft the pending snapshot is applied.
		l.pendingSnapshot = nil
		// FIXME: Shall I update right here? or in handleInstallSnapshot?
		l.lastIncludedIndex = rd.Snapshot.Metadata.Index
		l.lastIncludedTerm = rd.Snapshot.Metadata.Term
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}

	// rn.Raft.Logger.AdvanceRaft(oldStabled, oldApplied, oldLastIncludedIndex,
	// 	oldLastIncludedTerm, prevTerm, prevVote, prevCommit)
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
