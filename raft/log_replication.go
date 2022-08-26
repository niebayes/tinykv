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
	"math"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// if log consistency says that it's term conflicts, set LogTerm = TermConflict in the response msg.
var TermConflict uint64 = math.MaxUint64

// handle MsgPropose message.
func (r *Raft) handlePropose(msg pb.Message) {
	r.mustAppendEntries(msg.Entries)
	// followers will drop this msg.
	// candidates only append entries.
	// leader append entries and broadcast them out.
	if r.State == StateLeader {
		r.bcastAppendEntries()
	}
}

func (r *Raft) bcastAppendEntries() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(req pb.Message) {
	// note, stale msgs are dropped in Step.

	// since this msg is not dropped, I admit the from node is the current leader,
	// so I step down and reset election timer to not compete with him.
	r.becomeFollower(req.Term, req.From)
	r.resetElectionTimer()

	// Index and LogTerm in AppendEntries msg are interpreted as prevLogIndex and prevLogTerm in raft paper.
	prevLogIndex := req.Index
	prevLogTerm := req.LogTerm

	reject := true

	ok, nextIndex, conflictTerm := r.logConsistencyCheck(prevLogIndex, prevLogTerm)
	if ok {
		reject = false

		// since followers simply duplicate the leader's log,
		// and hence if an existing entry conflicts with the entries received from the leader,
		// delete the conflicting entry and all entries follow it in followers.

		hasConflict := false
		for li, leadEntry := range req.Entries {
			for mi, myEntry := range r.RaftLog.entries {
				// a conflicting entry is such an entry with the same index but with different term.
				if leadEntry.Index == myEntry.Index && leadEntry.Term != myEntry.Term {
					// TODO
					// committed entries never get discarded.
					// if mi < int(r.RaftLog.committed) {
					// 	mi = int(r.RaftLog.committed)
					// }

					// discard conflicting entry and ones follow it, and then append.
					r.RaftLog.entries = r.RaftLog.entries[:mi]
					r.appendEntries(req.Entries[li:])
					hasConflict = true
				}
			}
		}

		// no conflict, append all.
		if !hasConflict {
			r.appendEntries(req.Entries)
		}
	}

	// update commit index.
	if req.Commit > r.RaftLog.committed {
		// commit index cannot go over the index of the last entry in this node.
		r.RaftLog.committed = min(req.Commit, r.RaftLog.LastIndex())
	}

	// construct the response.
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      req.From,
		From:    r.id,
		Index:   nextIndex,
		LogTerm: conflictTerm,
		// forward back the entries to let the leader know it's these entries are rejected or not rejected.
		Entries: req.Entries,
		Reject:  reject,
	}

	// send the response.
	r.forwardMsgUp(res)

	// it might take a long time to append and persist logs,
	// so reset timer again to make the system more robust.
	r.resetElectionTimer()
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

	prs, ok := r.Prs[res.From]
	// if the leader thought this peers goes down for some reason, drop this message.
	if !ok {
		return
	}

	// Reject = true if log consistency check fails
	if res.Reject {
		// TODO
	} else {
		prs.Next = prs.Next + uint64(len(res.Entries))
	}

	// update the peer's progress in the leader's view.
	prs.Match = prs.Next - 1

	// if a majority of followers has not rejected the entries, the entries are committed in the leader.
	r.maybeUpdateCommitIndex()
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

// the entries ents are appended whatsoever.
// generally called when raft module receives new log entries from the upper application.
func (r *Raft) mustAppendEntries(ents []*pb.Entry) {
	for _, entry := range ents {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
}

// check that this node contains an entry at prevLogIndex whose term matches prevLogTerm.
// otherwise, the leader's log and the follower's log would be inconsistent if appended these entries.
// return (false, nextIndex, conflictTerm) if not pass the consistency check.
// return (true, 0, 0) if pass the consistency check.
func (r *Raft) logConsistencyCheck(prevLogIndex, prevLogTerm uint64) (ok bool, nextIndex uint64, conflictTerm uint64) {
	// empty logs are always consistent no matter of what log this server has.
	// since this server simply duplicates leader's log by discarding any conflict logs.
	if prevLogIndex == 0 {
		return true, 0, 0
	}

	// TODO: Add snapshot related logic.

	// index conflicts. If accept this AENT, it leaves a hole in this node's log.
	if r.RaftLog.LastIndex() < prevLogIndex {
		nextIndex = r.RaftLog.LastIndex() + 1
		return false, nextIndex, 0
	}

	// term conflicts.

	// TODO: rewrite to add snapshot related logic.
	myTerm := r.RaftLog.entries[prevLogIndex].Term
	if myTerm != prevLogTerm {
		conflictTerm = myTerm
		for index := prevLogIndex; index >= 1; index-- {
			if r.RaftLog.entries[index].Term == conflictTerm {
				nextIndex = index
			} else {
				return false, nextIndex, conflictTerm
			}
		}
	}

	return true, 0, 0
}

func (r *Raft) agreedWithMajority(index uint64) bool {
	cnt := 1 // the leader has already replicated the log entry.
	for id, prs := range r.Prs {
		if id != r.id {
			if prs.Match >= index {
				cnt++
			}
		}
	}
	return 2*cnt > len(r.peers)
}

func (r *Raft) maybeUpdateCommitIndex() {
	for index := r.RaftLog.LastIndex(); index > r.RaftLog.committed; index-- {
		// only commit entries at the current term.
		if r.RaftLog.entries[index].Term != r.Term {
			continue
		}
		if r.agreedWithMajority(index) {
			r.RaftLog.committed = index
			break
		}
	}
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
}

func (r *Raft) makeEntries(to uint64) []*pb.Entry {
	prs, ok := r.Prs[to]
	if !ok {
		return nil
	}

	ents0 := r.RaftLog.sliceEntsStartAt(prs.Next)
	ents := make([]*pb.Entry, 0)
	for _, entry := range ents0 {
		ents = append(ents, &entry)
	}
	return ents
}

// the leader sends each peer in the cluster except itself the log entries it thinks they need
// according to each peer's progress.
func (r *Raft) makeAppendEntries(to uint64) pb.Message {
	entries := r.makeEntries(to)
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
