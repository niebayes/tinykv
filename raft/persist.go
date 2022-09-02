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
	// so we need to update the applied index and stabled index accordingly.
	index := hardState.Commit
	l.committed = index
	l.applied = index
	l.stabled = max(index, l.stabled)
}

func (r *Raft) restoreConfState(confState *pb.ConfState) {
	for _, id := range confState.Nodes {
		if _, ok := r.Prs[id]; !ok {
			r.Prs[id] = r.newProgress()
		}
	}
}

func (r *Raft) newProgress() *Progress {
	return &Progress{
		Next:  r.RaftLog.LastIndex(),
		Match: 0,
	}
}
