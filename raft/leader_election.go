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

// becomeFollower transform this peer's state to Follower
// @param term
// @param lead the node with id lead is believed to be the current leader.
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// FIXME: Shall I differentiate between step down and become follower?
	if term > r.Term {
		r.Term = term
		r.resetVoteRecord()
	}
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State != StateFollower && r.State != StateCandidate {
		panic("invalid state transition")
	}
	// increment term.
	r.Term++

	// vote for self.
	r.resetVoteRecord()
	r.Vote = r.id
	r.votes[r.id] = true

	// reset election timer.
	r.resetElectionTimer()

	// now it's safe to become a candidate.
	r.State = StateCandidate
}

// upon becoming a new candidate, broadcast RequestVote RPCs to start a new round of election.
func (r *Raft) bcastRequestVote() {
	for _, to := range r.peers {
		// skip myself.
		if to != r.id {
			r.sendRequestVote(to)
		}
	}
}

// handle RequestVote RPC request.
func (r *Raft) handleRequestVote(req pb.Message) {
	// if I'm stale, keep pace with the candidate.
	if req.Term > r.Term {
		// update my term, since
		r.becomeFollower(req.Term, req.From)
	}

	reject := true // reject granting the vote?
	// grant vote only if he is not stale.
	// FIXME: Shall check state? For e.g. only candidate or follower can grant vote?
	if req.Term >= r.Term {
		// r.Vote == req.From says: since the network may duplicate the RPC request,
		// I have to vote for the same candidate once again if necessary.
		if r.Vote == None || r.Vote == req.From {
			r.Vote = req.From
			reject = false
		}
	}

	// reset election timer since I've granted the vote and this may result in spawning a new leader.
	// I don't want to compete with the new leader.
	if !reject {
		r.resetElectionTimer()
	}

	// construct RequestVote response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      req.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	// send the response.
	r.msgs = append(r.msgs, res)
}

// handle RequestVote RPC response.
func (r *Raft) handleRequestVoteResponse(res pb.Message) {
	if res.Term > r.Term {
		r.becomeFollower(res.Term, res.From)
		// stop voting if I step down.
		return
	}

	if r.State != StateCandidate {
		// stop voting if I'm not a candidate any more. I.e. I'm the leader and this response is stale.
		return
	}

	// whatever the from node voted for or against, record it.
	r.votes[res.From] = !res.Reject

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
	if 2*num_supports > len(r.peers) {
		r.becomeLeader()
		// upon becoming a new leader, broadcast a no-op entry to claim the leadership
		// and keep other nodes' log in sync.
		r.bcastAppendEntriesNoop()
		return
	}

	// a majority of nodes in the cluster reject me, I step down.
	if 2*num_denials > len(r.peers) {
		r.becomeFollower(r.Term, res.From)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.resetVoteRecord()
	r.resetPeerProgress()
	r.State = StateLeader
}

// upon becoming a new leader, broadcast a no-op entry to claim the leadership
// and keep other servers' log in sync.
func (r *Raft) bcastAppendEntriesNoop() {
	for _, to := range r.peers {
		// skip myself.
		if to != r.id {
			r.sendAppendEntriesNoop(to)
		}
	}
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

func (r *Raft) makeRequestVote(to uint64) pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	return msg
}

func (r *Raft) sendRequestVote(to uint64) {
	msg := r.makeRequestVote(to)
	r.forwardMsgUp(msg)
}

// reset progress of each peer except this node itself.
// called when the node boots up or wins an election.
func (r *Raft) resetPeerProgress() {
	for _, id := range r.peers {
		r.Prs[id] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
	}
}

// the tests assume a no-op entry is an entry with nil Data.
func (r *Raft) makeNoopEntry() []*pb.Entry {
	noop_entry := make([]*pb.Entry, 0)
	noop_entry = append(noop_entry, &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	return noop_entry
}

func (r *Raft) makeAppendEntriesNoop(to uint64) pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: r.makeNoopEntry(),
	}
	return msg
}

func (r *Raft) sendAppendEntriesNoop(to uint64) {
	msg := r.makeAppendEntriesNoop(to)
	r.forwardMsgUp(msg)
}
