package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleTransferLeader(m pb.Message) {
	r.Logger.recvTRAN(m.From)
	// TransferLeader is a local msg which has term equals to 0, so don't drop it by comparing terms.

	// there's a pending leader transfer task, ignore this msg.
	if r.leadTransferee != None {
		return
	}

	// by convention, the upper application set m.From to the transfer target's id.
	pr, ok := r.Prs[m.From]

	// cannot find the transfer target in the leader's view, ignore this msg.
	if !ok {
		return
	}

	// start transfering. Proposal receiving is paused until this transfering is done or aborted.
	r.leadTransferee = m.From
	r.resetTransferTimer()

	// if the transfer target already catched up with me, immediately send TimeoutNow msg.
	l := r.RaftLog
	if pr.Match >= l.committed {
		r.sendTimeoutNow(m.From)
	}
	// otherwise, we have to wait for it catching up with me, or abort the transfering due to transfer time out.
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

	// drop stale msgs.
	if m.Term < r.Term {
		return
	}

	// I admit the from node is the current leader, so I step down and reset election timer
	// to not compete with it.
	// FIXME: will this term changing affects up-to-date log checking?
	r.becomeFollower(m.From, m.Term)
	r.resetElectionTimer()

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
