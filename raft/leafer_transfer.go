package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleTransferLeader(m pb.Message) {
	r.Logger.recvTRAN(m.From)
	// TransferLeader is a local msg which has term equals to 0, so don't drop it by comparing terms.

	// FIXME: Shall a follower redirects TransferLeader?
	// TODO: figure out which msgs shall be redirected to leader?
	if r.State == StateFollower {
		if r.Lead != None {
			m.To = r.Lead
			r.forwardMsgUp(m)
		}
		return
	}

	// the leadership transferring to node m.From is in progress, wait for a while.
	if r.leadTransferee == m.From {
		return
	}
	// start a new round of leadership transfer even when there's a pending transferring to another node.

	// by convention, the upper application set m.From to the transfer target's id.
	pr, ok := r.Prs[m.From]

	// cannot find the transfer target in the leader's view, ignore this msg.
	if !ok {
		return
	}

	// the transfer target is me and I am the leader, ignore this msg.
	if m.From == r.id && r.State == StateLeader {
		return
	}

	// start transfering. Proposal receiving is paused until this transfering is done or aborted.
	r.leadTransferee = m.From
	r.resetTransferTimer()

	// the transfer target is me and I am not the leader, and since I must have caught up with me.
	// so immediately send TimeoutNow to myself.
	if m.From == r.id && r.State != StateLeader {
		r.sendTimeoutNow(m.From)
	}

	// if the transfer target already catched up with me, immediately send TimeoutNow msg.
	l := r.RaftLog
	// FIXME: Shall I compare Match with commit index or last log index?
	// etcd chooses the later one, but the raft phd paper seems suggests the former one.
	if pr.Match >= l.committed {
		r.sendTimeoutNow(m.From)
	} else {
		// otherwise, we have to wait for it catching up with me, or abort the transfering due to transfer time out.
		r.sendAppendEntries(m.From, true)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	l := r.RaftLog
	lastLogIndex := l.LastIndex()
	lastLogTerm, _ := l.Term(lastLogIndex)
	r.forwardMsgUp(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		// the transfer target will drop this msg if its commit index < Commit,
		// or if its log is not at least up-to-date as me.
		Commit:  r.RaftLog.committed,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
	})
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	r.Logger.recvTNOW(m)

	// TODO: drop all msgs from nodes that not currently in the same raft group as me.
	// FIXME: Is this reasonable? Shall a msg from another raft group steps down me?

	// I was removed from the raft group, drop this msg.
	// TODO: wrap to a helper function to be reused for others.
	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	// drop stale msgs.
	if m.Term < r.Term {
		return
	}

	// I admit the from node is the current leader, so I step down and reset election timer
	// to not compete with it.
	// FIXME: will this term changing affects up-to-date log checking?
	r.becomeFollower(m.From, m.Term)
	r.resetElectionTimer()

	// FIXME: Shall I do all of these check to prove my qualification?
	// etcd does not do so.

	// ensure I'm qualified to be the new leader:
	// I have committed what leader committed.
	l := r.RaftLog
	if m.Commit > l.committed {
		return
	}

	// and I have at least as up-to-date log as the current leader's,
	lastLogIndex := m.Index
	lastLogTerm := m.LogTerm
	li := l.LastIndex()
	lt, _ := l.Term(li)
	if (lt > lastLogTerm) || (lt == lastLogTerm && li >= lastLogIndex) {
		// yes, indeed. I'm qualified.
		r.Logger.leaderQualified()
		// send a hup to myself which will start a new election ASAP even if the election timer is not elasped.
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
		})
	}
}

// start transfer time upon receving a TransferLeader request.
func (r *Raft) resetTransferTimer() {
	r.transferElapsed = 0
}
