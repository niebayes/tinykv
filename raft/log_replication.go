//
// this file contains methods related to log replication.
//
// the typical route of log replication is:
//		upper application proposes a set of entries to be replicated
//	bcastAppendEntries
//		other peers: handleAppendEntries
//	handleAppendEntriesResponse
//		leader knows which entries are committed
//	bcastHeartbeat
//		other peers know which entries are committed
// 	handleHeartbeatResponse
//	bcastHeartbeat
//		...
//		upper application proposes a set of entries to be replicated
//

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) bcastAppendEntries() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(req pb.Message) {
	// step down if I'm stale.
	if req.Term > r.Term {
		r.becomeFollower(req.Term, req.From)
	}

	// reject if the from node is stale.
	reject := false
	if req.Term < r.Term {
		reject = true
	}

	// FIXME: Is this strategy correct?
	if !reject {
		// I admit the from node is the current leader, so I step down and reset election timer
		// to not compete with him.
		r.becomeFollower(req.Term, req.From)
		r.resetElectionTimer()

		r.appendEntries(req.Entries)
	}

	// construct the response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      req.From,
		From:    r.id,
		Reject:  reject,
	}

	// send the response.
	r.msgs = append(r.msgs, res)
}

// handle AppendEntries RPC response.
func (r *Raft) handleAppendEntriesResponse(res pb.Message) {
	// step down if I'm stale.
	if res.Term > r.Term {
		r.becomeFollower(res.Term, res.From)
		return
	}

	// only leader is responsible to process AppendEntries response.
	if r.State != StateLeader {
		return
	}
}

func (r *Raft) bcastHeartbeat() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(req pb.Message) {
	// step down if I'm stale.
	if req.Term > r.Term {
		r.becomeFollower(req.Term, req.From)
	}

	// reject if the from node is stale.
	reject := false
	if req.Term < r.Term {
		reject = true
	}

	if !reject {
		// I admit the from node is the current leader, so I step down and reset election timer
		// to not compete with him.
		r.becomeFollower(req.Term, req.From)
		r.resetElectionTimer()
	}

	// construct the response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      req.From,
		From:    r.id,
		Reject:  reject,
	}

	// send the response.
	r.msgs = append(r.msgs, res)
}

// handle Heartbeat RPC response.
func (r *Raft) handleHeartbeatResponse(res pb.Message) {

}

//
// utilities.
//

func (r *Raft) appendEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
}

func (r *Raft) makeEntries() []*pb.Entry {
	entries := make([]*pb.Entry, 0)
	for _, entry := range r.RaftLog.entries {
		entries = append(entries, &entry)
	}
	return entries
}

func (r *Raft) makeAppendEntries(to uint64) pb.Message {
	entries := r.makeEntries()
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: entries,
	}
	return msg
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	msg := r.makeAppendEntries(to)
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) makeHeartbeat(to uint64) pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		// heartbeat conveys empty entries.
		// the test suites require it to be nil rather than empty slice
		Entries: nil,
		Index:   0,
		LogTerm: 0,
	}
	return msg
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := r.makeHeartbeat(to)
	r.msgs = append(r.msgs, msg)
}
