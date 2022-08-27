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
	// followers will drop this msg. (Possibly they will redirect it to the leader)
	// candidates only append entries.
	// leader append entries and broadcast them out.
	if r.State == StateLeader {
		r.bcastAppendEntries()
	}
}

// broadcast AppendEntries RPCs to all peers (except me) the leader thinks it's alive
func (r *Raft) bcastAppendEntries() {
	for _, to := range r.peers {
		if to != r.id {
			r.sendAppendEntries(to)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		// drop stale msgs.
		return
	}

	// since this msg is not dropped, I admit the from node is the current leader,
	// so I step down and reset election timer to not compete with him.
	r.becomeFollower(m.Term, m.From)
	r.resetElectionTimer()

	// Index and LogTerm in AppendEntries msg are interpreted as prevLogIndex and prevLogTerm in raft paper.
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	l := r.RaftLog

	// log consistency check: log has an entry at index prevLogIndex and with the same term.
	ent, err := l.Entry(prevLogIndex)
	if err != nil {
		// my log does not have a entry at the index prevLogIndex, and hence index conflicts.
		// I don't have an entry at index prevLogIndex and I even don't know whether I have entries
		// with indexes < prevLogIndex. So the only hint I can give to the leader is the index
		// of the last entry in my log. Hopefully, the next set of entries the leader gives to me
		// start from (last index + 1), which are the entries what I'm missing right now.
		r.forwardMsgUp(pb.Message{
			MsgType:   pb.MessageType_MsgAppendResponse,
			To:        m.From,
			From:      r.id,
			Index:     prevLogIndex, // to indicate it's which MsgAppend was rejected.
			Reject:    true,
			Reason:    pb.RejectReason_IndexConflict,
			NextIndex: l.LastIndex() + 1,
		})
		return

	} else if ent.Term != prevLogTerm {
		// my log has an entry at the index prevLogIndex but with a different term, and hence term conflicts.
		// this is because I have appended some entries during the conflict term,
		// and it turns out that I shall not append those entries and all entris appended during the
		// conflict term must be discarded.
		// so I suggest the leader the next set of entries you give me shall be the entries
		// that the first entry's term is not the conflict term.
		nextIndex := prevLogIndex
		conflictTerm := ent.Term
		for index := prevLogIndex - 1; index > l.lastIncludedIndex; index-- {
			ent, err = l.Entry(index)
			if err != nil || ent.Term != conflictTerm {
				break
			}
			nextIndex--
		}

		r.forwardMsgUp(pb.Message{
			MsgType:      pb.MessageType_MsgAppendResponse,
			To:           m.From,
			From:         r.id,
			Index:        prevLogIndex, // to indicate it's which MsgAppend was rejected.
			Reject:       true,
			Reason:       pb.RejectReason_TermConflict,
			NextIndex:    nextIndex,
			ConflictTerm: conflictTerm,
		})
		return
	}

	// the new entries are not rejected if passed the log consistency check.
	// but some of them may be stale, some of them may have conflict,
	// so I have to skip conflicts and append the actually new entries which I don't have.
	lastNewEntryIndex := l.appendNewEntries(m.Entries)

	// update commit index.
	if m.Commit > l.committed {
		// committed entries must be those entries that I have
		l.committed = min(m.Commit, lastNewEntryIndex)
	}

	r.forwardMsgUp(pb.Message{
		MsgType:   pb.MessageType_MsgAppendResponse,
		To:        m.From,
		From:      r.id,
		Index:     lastNewEntryIndex, // used for leader to update the peer's Next. Only used in test suites.
		Reject:    false,
		NextIndex: lastNewEntryIndex + 1,
	})

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
	li := r.RaftLog.LastIndex()
	for i := 0; i < len(ents); i++ {
		entry := *ents[i]
		entry.Index = li + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}
}

// check that this node contains an entry at prevLogIndex whose term matches prevLogTerm.
// otherwise, the leader's log and the follower's log would be inconsistent if appended these entries.
// return (false, nextIndex, conflictTerm) if not pass the consistency check.
// return (true, 0, 0) if pass the consistency check.
func (r *Raft) checkLogConsistency(prevLogIndex, prevLogTerm uint64) (ok bool, nextIndex uint64, conflictTerm uint64) {
	// empty logs are always consistent no matter of what log this server has.
	// since this server simply duplicates leader's log by discarding any conflict logs.
	if prevLogIndex == 0 {
		return true, 0, 0
	}

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

func (r *Raft) sendAppend(to uint64) bool {
	msg := r.makeAppendEntriesNoop(to)
	// only for test purpose: update nextIndex optimistically.
	// prs, ok := r.Prs[to]
	// if !ok {
	// 	panic("send to unknown peer")
	// }
	// prs.Next = prs.Next + uint64(len(msg.Entries))
	r.forwardMsgUp(msg)
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppendEntries(to uint64) bool {
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
