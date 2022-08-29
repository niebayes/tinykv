//
// this file contains methods related to leader election.
//
// the typical route of leader election is:
// 	becomeFollower
//		election time out
//	becomeCandidate
//  bcastRequestVote
//		other peers: handleRequestVote
//	handleRequestVoteResponse
//		receive a majority of votes
// 	becomeLeader
//	bcastAppendEntriesNoop
// 	bcastHeartBeat
//		...
//		stale
//	becomeFollower
//
package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// handle MsgHup message
func (r *Raft) handleMsgHup() {
	r.logger.recvHUP()

	r.becomeCandidate()
	// if the cluster only contain one node, this node immediately becomes the leader.
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	// otherwise, there must be a majority of votes from the cluster to become the leader.
	r.bcastRequestVote()
}

// handle MsgBeat message.
func (r *Raft) handleBeat() {
	r.logger.recvBEAT()

	r.bcastHeartbeat()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	oldTerm := r.Term

	// FIXME: Shall I differentiate between step down and become follower?
	if term > r.Term {
		r.Term = term
		r.resetVoteRecord()
	}
	r.Lead = lead
	r.State = StateFollower

	r.logger.stateToFollower(oldTerm)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// increment term.
	r.Term++

	// vote for self.
	r.resetVoteRecord()
	r.Vote = r.id
	r.votes[r.id] = true

	// reset election timer.
	r.resetElectionTimer()

	// now it's safe to become a candidate.
	r.logger.stateToCandidate()
	r.State = StateCandidate
}

// upon becoming a new candidate, broadcast RequestVote RPCs to start a new round of election.
func (r *Raft) bcastRequestVote() {
	r.logger.bcastRVOT()

	for to := range r.Prs {
		// skip myself.
		if to != r.id {
			r.forwardMsgUp(pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      to,
				Term:    r.Term,
			})
		}
	}
}

// handle RequestVote RPC request.
func (r *Raft) handleRequestVote(m pb.Message) {
	r.logger.recvRVOT(m)

	// if I'm stale, keep pace with the candidate.
	if m.Term > r.Term {
		// update my term, since
		r.becomeFollower(m.Term, m.From)
	}

	reject := true // reject granting the vote?
	// grant vote only if he is not stale.
	// FIXME: Shall check state? For e.g. only candidate or follower can grant vote?
	if m.Term >= r.Term {
		// r.Vote == req.From says: since the network may duplicate the RPC request,
		// I have to vote for the same candidate once again if necessary.
		if r.Vote == None || r.Vote == m.From {
			r.Vote = m.From
			reject = false
		}
	}

	if !reject {
		r.logger.voteTo(m.From)
	} else {
		r.logger.rejectVoteTo(m.From)
	}

	// reset election timer since I've granted the vote and this may result in spawning a new leader.
	// I don't want to compete with the new leader.
	if !reject {
		r.resetElectionTimer()
	}

	// send the response.
	r.forwardMsgUp(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

// handle RequestVote RPC response.
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.logger.recvRVOTRes(m)

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		// stop voting if I step down.
		return
	}

	if r.State != StateCandidate {
		// stop voting if I'm not a candidate any more. I.e. I'm the leader and this response is stale.
		return
	}

	// whatever the from node voted for or against, record it.
	r.votes[m.From] = !m.Reject

	// FIXME: Shall I differentiate between supports and denials?
	num_supports := 0 // number of nodes supporting me to become the leader.
	num_denials := 0  // number of nodes rejecting me to become the leader.
	for _, support := range r.votes {
		if support {
			num_supports++
		} else {
			num_denials++
		}
	}

	// a majority of nodes in the cluster support me, I become the new leader.
	if 2*num_supports > len(r.Prs) {
		r.becomeLeader()
		r.bcastAppendEntries(true)
		return
	}

	// a majority of nodes in the cluster reject me, I step down.
	if 2*num_denials > len(r.Prs) {
		r.becomeFollower(r.Term, m.From)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.logger.stateToLeader()

	r.resetVoteRecord()
	r.resetPeerProgress()
	r.State = StateLeader

	// upon becoming a new leader, broadcast a no-op entry to claim the leadership
	// and keep other nodes' log in sync.
	// upon becoming a new leader, immediately append a no-op entry and then broadcast it out
	// to claim the leadership and keep other peers' log in sync quickly.
	// when this no-op entry is committed, all entries including entries of previous terms
	// are also committed.
	noop_ent := r.makeNoopEntry()
	r.appendEntries([]*pb.Entry{&noop_ent})
	r.logger.appendEnts([]pb.Entry{noop_ent})
}

//
// utilities.
//

func (r *Raft) resetVoteRecord() {
	r.votes = make(map[uint64]bool)
	r.Vote = None
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	// raft introduces randomization into election timer to resolve split vote faster.
	r.electionTimeout = r.electionTimeoutBase + (rand.Int() % r.electionTimeoutBase)
}

// reset progress of each peer except this node itself.
// called when the node boots up or wins an election.
func (r *Raft) resetPeerProgress() {
	l := r.RaftLog
	for _, pr := range r.Prs {
		pr.Next = l.LastIndex() + 1
		pr.Match = 0
	}
}

// the tests assume a no-op entry is an entry with nil Data.
func (r *Raft) makeNoopEntry() pb.Entry {
	ent := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	}
	return ent
}
