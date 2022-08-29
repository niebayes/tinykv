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
	lastIncludedTerm  uint64

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
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		// append a dummy entry while init to simplify log indexing.
		entries:         make([]pb.Entry, 1),
		pendingSnapshot: &pb.Snapshot{},
	}

	// upon init, read all persisted stable logs into the log.
	fi, err := storage.FirstIndex()
	li, _ := storage.LastIndex()
	// err != nil => there's at least one entry excluding the dummy entry in the storage.
	if err == nil {
		stable_ents, _ := storage.Entries(fi, li+1)
		raft_log.entries = append(raft_log.entries, stable_ents...)
	}

	// update stabled index since we may have restored some persisted entries from stable storage.
	// note, commit index cannot be updated right now. Commit index is only updated when the leader
	// notifies us that a quorum of nodes in the cluster has replicated these entries.
	raft_log.stabled = raft_log.LastIndex()

	return raft_log
}

// return all entries not compacted yet, excluding the dummy entry.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	offset := l.idx2off(l.stabled + 1)
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// TODO: Add error handling.
	begin := l.idx2off(l.applied + 1)
	end := l.idx2off(l.committed)
	// note, slicing in Go is half open, i.e. left included, right excluded.
	return l.entries[begin : end+1]
}

// return a slice of log entries start at index i.
func (l *RaftLog) sliceStartAt(i uint64) []pb.Entry {
	if i <= l.lastIncludedIndex || i > l.LastIndex() {
		return nil
	}

	offset := l.idx2off(i)
	return l.entries[offset:]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return max(l.entries[len(l.entries)-1].Index, l.lastIncludedIndex)
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
func (l *RaftLog) Entry(i uint64) (*pb.Entry, error) {
	if i <= l.lastIncludedIndex || i > l.LastIndex() {
		return nil, ErrIndexOutOfRange
	}
	offset := l.idx2off(i)
	return &l.entries[offset], nil
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i < l.lastIncludedIndex || i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	if i == l.lastIncludedIndex {
		return l.lastIncludedTerm, nil
	}

	ent, _ := l.Entry(i)
	return ent.Term, nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}
