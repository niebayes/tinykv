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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
// snapshot is indicated by the lastIncludedIndex.
// generally, lastIncludedIndex + 1 = storage.FirstIndex
// the first and last are storage.FirstIndex and storage.LastIndex, respectively.
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

var ErrIndexOutOfRange error = errors.New("index out of range")
var ErrDiscardCommitted error = errors.New("discarding committed entries")

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// i.e. truncated log entries are not contained in the stable storage.
	// and we have:
	// lastIncludedIndex = storage.FirstIndex + 1, if storage is not empty.
	// stabled index = storage.LastIndex
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
	l := &RaftLog{
		storage:           storage,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		applied:           0,
		committed:         0,
		stabled:           0,
		// append a dummy entry while init to simplify log indexing.
		entries:         make([]pb.Entry, 1),
		pendingSnapshot: &pb.Snapshot{},
	}

	fi, _ := storage.FirstIndex()
	li, _ := storage.LastIndex()

	// retrieve truncated state.
	l.lastIncludedIndex = fi - 1
	l.lastIncludedTerm, _ = storage.Term(fi - 1)
	// these assignments are not necessary.
	l.entries[0].Index = l.lastIncludedIndex
	l.entries[0].Term = l.lastIncludedTerm

	// restore stable entries from storage.
	// note, Entries takes a range [lo, hi) with left closed and right open.
	stable_ents, err := storage.Entries(fi, li+1)
	if err == nil && len(stable_ents) > 0 {
		l.entries = append(l.entries, stable_ents...)
	}

	// update stabled index since we may have restored some persisted entries from stable storage.
	// note, commit index cannot be updated right now. Commit index is only updated when the leader
	// notifies us that a quorum of nodes in the cluster has replicated these entries.
	l.stabled = l.LastIndex()

	return l
}

// return all entries not compacted yet, excluding the dummy entry.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	index := l.stabled + 1
	if !l.isValidIndex(index) {
		// the creepy test requires us to return a empty slice, instead of nil.
		return make([]pb.Entry, 0)
	}
	offset := l.idx2off(index)
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	ents := l.allEntries()
	if len(ents) == 0 {
		return nil
	}
	// FIXME: Do I need to modify this to accomodate snapshotting?
	begin := max(l.applied+1, ents[0].Index)
	end := min(l.committed, l.LastIndex())
	if begin > end {
		return nil
	}
	begin = l.idx2off(begin)
	end = l.idx2off(end)
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

func (l *RaftLog) sliceEndAt(i uint64) []pb.Entry {
	if i <= l.lastIncludedIndex {
		return nil
	}
	ents := l.allEntries()
	if len(ents) == 0 || i < ents[0].Index {
		return nil
	}
	// note, slice is Go is left closed and right open.
	i = min(i, l.LastIndex())
	offset := l.idx2off(i)
	return ents[:offset+1]
}

// LastIndex return the last index of the log entries
// TODO: use the ent[0] to store lastIncludedIndex and lastIncludedTerm.
// TODO: ensure this function is correct.
func (l *RaftLog) LastIndex() uint64 {
	return max(l.entries[len(l.entries)-1].Index, l.lastIncludedIndex)
}

// transform log index to offset in the entries array.
func (l *RaftLog) idx2off(index uint64) uint64 {
	if !l.isValidIndex(index) {
		log.Fatalf(ErrIndexOutOfRange.Error())
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
	if !l.isValidIndex(i) {
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

func (l *RaftLog) hasPendingSnapshot() bool {
	// FIXME: Shall I also check snap.Data is not nil?
	return !IsEmptySnap(l.pendingSnapshot)
}

func (l *RaftLog) isValidIndex(i uint64) bool {
	return l.lastIncludedIndex < i && i <= l.LastIndex()
}
