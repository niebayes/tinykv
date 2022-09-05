package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) sendInstallSnapshot(to uint64) {
	l := r.RaftLog
	snap, err := l.storage.Snapshot()
	// the snapshot is generating, retry later.
	if err == ErrSnapshotTemporarilyUnavailable {
		return
	}
	// unknown error occurs.
	// FIXME: Shall I panic? Failed for 5 times to get snapshot will raise an error as well
	if err != nil || IsEmptySnap(&snap) {
		panic(err)
	}

	// FIXME: Does InstallSnapshot RPC convey commit index?
	r.forwardMsgUp(pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snap,
	})

	r.Logger.sendSnap(to, &snap)
}

// this function gets only called by tester.
func (r *Raft) handleSnapshot(m pb.Message) {
	r.handleInstallSnapshot(m)
}

// handle InstallSnapshot request.
func (r *Raft) handleInstallSnapshot(m pb.Message) {
	// FIXME: risk on logging nil snapshot?
	r.Logger.recvSNAP(m)

	// drop stale msgs.
	if m.Term < r.Term {
		return
	}

	// since InstallSnapshot RPC only flows from leader to follower,
	// I admit the from node is the current leader.
	// so I step down and reset election timer to not compete with it.
	r.becomeFollower(m.Term, m.From)
	r.resetElectionTimer()

	snap := m.Snapshot
	if IsEmptySnap(snap) {
		panic("empty snapshot")
	}

	l := r.RaftLog

	// ignore stale snapshot.
	// this is not necessary iff we do not decrease commit index and applied index.
	if snap.Metadata.Index <= l.committed {
		return
	}

	// do not install this snapshot, if there's already a pending snapshot.
	if l.hasPendingSnapshot() {
		return
	}

	si := snap.Metadata.Index // snapshot index.
	st := snap.Metadata.Term  // the term of the entry at the snapshot index.
	// note there's a dummy entry.
	newEnts := make([]pb.Entry, 1)
	// these assignments are not necessary.
	newEnts[0].Index = si
	newEnts[0].Term = st

	// discard all entries before and including the snapshot index.
	// if no entry matches the snapshot index, than this append won't execute
	// and hence all entries are discarded.
	// note, snapshot corresponds to the state machine state.
	// for a log replicated state machine, in order to keep sync with all servers,
	// all servers execute the same cmds in the same order, and they all reach
	// the same state eventually.
	// note, the goal is the state machine state, not having to execute all log entries.
	// therefore upon receiving a snapshot, we can safely discard all log entries before and including
	// the snapshot index.

	for i := 1; i < int(l.Len()); i++ {
		ent := l.entries[i]
		if ent.Index == si && ent.Term == st {
			newEnts = append(newEnts, l.entries[i+1:]...)
		}
	}
	l.entries = newEnts

	r.Logger.entsAfterSnapshot()

	oldLastIncludedIndex := l.lastIncludedIndex
	oldLastIncludedTerm := l.lastIncludedIndex
	oldCommitted := l.committed
	oldApplied := l.applied
	oldStabled := l.stabled

	// install this snapshot on raft module. The peer storage is responsible for installing
	// the snapshot on stable storage.
	// TODO: wrap the following to a function InstallSnapshot
	l.pendingSnapshot = snap
	if l.lastIncludedIndex < si {
		l.lastIncludedTerm = st
	}
	l.lastIncludedIndex = max(l.lastIncludedIndex, si)
	l.committed = max(l.committed, si) // never decrease commit index.
	// TODO: Delay the update of applied index and stabled index to the peer.
	l.applied = max(l.applied, si) // never decrease applied index.
	// TODO: fix stabled index update logic.
	l.stabled = max(l.stabled, si)
	if snap.Metadata.ConfState != nil {
		// always update conf state?
		r.restoreConfState(snap.Metadata.ConfState)
	}

	r.Logger.logStateAfterSnapshot(oldCommitted, oldStabled, oldApplied,
		oldLastIncludedIndex, oldLastIncludedTerm)

	// TODO: send AppendEntries response to tell the leader my progress.
	// this is not necessary.
	// FIXME: Shall I design a HandleInstallSnapshotResponse?
	r.forwardMsgUp(pb.Message{
		MsgType:   pb.MessageType_MsgAppendResponse,
		To:        m.From,
		From:      r.id,
		Term:      r.Term,
		Reject:    false,
		NextIndex: min(l.LastIndex(), l.committed),
	})
}
