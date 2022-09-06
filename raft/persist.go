//
// this file contains methods related to persistence.
//
package raft

import (
	// "errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) restoreHardState(hardState *pb.HardState) {
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	l := r.RaftLog
	// note, the raft init log index may not 0, and hence hardState.Commit may not be 0.
	// so we need to update the stabled index accordingly.
	// note, we cannot update applied index according to commit index, it only gets updated
	// when the state machine executes a raft cmd successfully.
	index := hardState.Commit
	l.committed = index
	l.stabled = max(index, l.stabled)

	r.Logger.restoreHardState(hardState)
}

func (r *Raft) restoreConfState(confState *pb.ConfState) {
	// clear and reset peer progresses.
	r.Prs = make(map[uint64]*Progress)
	for _, id := range confState.Nodes {
		if _, ok := r.Prs[id]; !ok {
			r.Prs[id] = r.newProgress()
		}
	}
	// reset vote record since config state changes.
	r.resetVoteRecord()

	r.Logger.restoreConfState(confState)
}

func (r *Raft) newProgress() *Progress {
	return &Progress{
		Next: r.RaftLog.LastIndex() + 1,
		// FIXME: Shall I adjust Match to raft init log index?
		Match: 0,
	}
}
