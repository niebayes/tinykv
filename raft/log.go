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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplicity the RaftLog implementation should manage all log entries
// that not truncated, i.e. not snapshoted.

// raft log has three constituents:
//		snapshot + stable log + unstable log
// snapshot: compacted log entries.
// stable log: persisted log entries.
// unstable log: to-to-persisted log entries.
//
// note, stable != commited. Even if log entries are persisted, they may not be committed yet
// and even may be discarded.

import (
	"errors"
)

var ErrIndexOutOfRange error = errors.New("index out of range")
var ErrDiscardCommitted error = errors.New("discarding committed entries")

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// I.e. log entries with index > stabled are not persisted yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	lastIncludedIndex uint64

	// all entries that have not yet compact, i.e. not snapshoted.
	// entries = stable log entries + unstable log entries.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	raft_log := &RaftLog{
		storage:           storage,
		committed:         0,
		applied:           0,
		stabled:           0,
		lastIncludedIndex: 0,
		// append a dummy entry while init to simplify log indexing.
		entries:         make([]pb.Entry, 1),
		pendingSnapshot: &pb.Snapshot{},
	}
	return raft_log
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries[l.stabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.entries[l.committed:]
}

// return a slice of log entries start at index i.
func (l *RaftLog) sliceEntsStartAt(i uint64) (ents []pb.Entry) {
	if i > l.LastIndex() {
		return nil
	}

	last_snapshot, err := l.storage.Snapshot()
	if err == nil {
		if i <= last_snapshot.Metadata.Index {
			return nil
		}
	}

	// TODO: Ensure the log entries are continuous.
	for _, entry := range l.entries {
		if entry.Index >= i {
			ents = append(ents, entry)
		}
	}

	return
}

// return the length of l.entries excluding the dummy entry.
func (l *RaftLog) LenUnCompacted() int {
	return len(l.entries) - 1
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	n := len(l.entries)
	if n > 0 {
		return l.entries[n-1].Index
	}
	// note, LastIndex always success since MemStorage has appended a dummy entry while init.
	// and if there's a snapshot, the dummy entry's index is set to the index of the last entry
	// in the snapshot.
	li, _ := l.storage.LastIndex()
	return li
}

// transform log index to offset in the entries array.
func (l *RaftLog) idx2off(index uint64) uint64 {
	if index < 1 {
		panic(ErrIndexOutOfRange)
	}

	// note, there's a dummy entry at the head of entries.
	offset := index - l.lastIncludedIndex
	return offset
}

// return the length of log entries including the dummy entry.
func (l *RaftLog) Len() uint64 {
	return uint64(len(l.entries))
}

// return (entry, nil) where the entry is the entry at index index if the entry exists. Otherwise, return (nil, error)
func (l *RaftLog) Entry(index uint64) (*pb.Entry, error) {
	offset := l.idx2off(index)
	if offset > l.Len() {
		return nil, ErrIndexOutOfRange
	}
	return &l.entries[offset], nil
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	var term uint64
	snapshot_index := uint64(0)
	last_snapshot, err := l.storage.Snapshot()
	if err == nil {
		snapshot_index = last_snapshot.Metadata.Index
		if i < snapshot_index {
			// ErrSnapOutOfDate says the requested entry at the given index is older than the existing snapshot
			// and so cannot be reached.
			return 0, ErrSnapOutOfDate
		} else if i == snapshot_index {
			term = last_snapshot.Metadata.Term
			return term, nil
		}
	}

	offset := int(i - snapshot_index)
	if offset < len(l.entries) {
		term = l.entries[offset].Term
		return term, nil
	}

	return 0, ErrUnavailable
}

// append the actually new entries that I don't have.
// if all entries are stale or conflicting, return 0.
// otherwise, return the index of last new entry.
func (l *RaftLog) appendNewEntries(nents []*pb.Entry) uint64 {
	// since followers simply duplicate the leader's log,
	// and hence if an existing entry conflicts with the entries received from the leader,
	// delete the conflicting entry and all entries follow it in followers.
	for i := 0; i < len(nents); i++ {
		nent := nents[i]                // leader's entry.
		ent, err := l.Entry(nent.Index) // my entry.
		// all entris before the conflict entry were appended to my log.
		// all entries after and including the conflict entry needs to be appended to my log;
		if err != nil || ent.Term != nent.Term {
			if ent.Index <= l.committed {
				panic(ErrDiscardCommitted)
			}
			for _, nent := range nents[i:] {
				l.entries = append(l.entries, *nent)
			}
		}
	}
	return 0
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}
