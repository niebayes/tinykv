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
	"sort"
)

// handle MsgPropose message.
func (r *Raft) handlePropose(m pb.Message) {
	r.Logger.recvPROP(m)

	// ignore the proposal if no entries.
	if len(m.Entries) == 0 {
		return
	}

	// annotate entries with the current term and the corresponding index.
	li := r.RaftLog.LastIndex()
	for i := 0; i < len(m.Entries); i++ {
		m.Entries[i].Index = li + uint64(i) + 1
		m.Entries[i].Term = r.Term
	}
	r.appendEntries(m.Entries)
	r.Logger.appendEnts(entsClone(m.Entries))

	// followers will drop MsgProp. (Possibly they will redirect it to the leader)
	// candidates only append entries.
	// leader append entries and broadcast them out.
	if r.State == StateLeader {
		// update the leader's own progress as the test requires.
		r.updateLeaderProg()

		// if there's only one node in the cluster, each MsgPropose would drive the update of the commit index.
		if updated := r.maybeUpdateCommitIndex(); updated || len(m.Entries) > 0 {
			r.bcastAppendEntries(true)
		}
	}
}

// broadcast AppendEntries RPC to all other peers in the cluster.
// if must is false and there's no new entries to send to a peer, don't send RPC to the peer.
// if must is true, the RPC is always sent and if there's no new entries, the latest snapshot is sent.
// if there's no available snapshot right, panic.
func (r *Raft) bcastAppendEntries(must bool) {
	r.Logger.bcastAENT()

	// note, the order of iterating map in Go is not determined.
	// someone says raft would be more stable if the bcast order is determined.
	ids := r.idsFromPrs()
	for _, to := range ids {
		if to != r.id {
			r.sendAppendEntries(to, true)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.Logger.recvAENT(m)

	// drop stale msgs.
	if m.Term < r.Term {
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
	if prevLogIndex > l.lastIncludedIndex {
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
				Term:      r.Term,
				Index:     prevLogIndex, // to indicate it's which MsgAppend was rejected.
				Reject:    true,
				Reason:    pb.RejectReason_IndexConflict,
				NextIndex: l.LastIndex() + 1,
			})

			r.Logger.rejectEnts(pb.RejectReason_IndexConflict, m.From)
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
				Term:         r.Term,
				Index:        prevLogIndex, // to indicate it's which MsgAppend was rejected.
				Reject:       true,
				Reason:       pb.RejectReason_TermConflict,
				NextIndex:    nextIndex,
				ConflictTerm: conflictTerm,
			})

			r.Logger.rejectEnts(pb.RejectReason_TermConflict, m.From)
			return
		}
	}

	r.Logger.acceptEnts(m.From)

	// the new entries are not rejected if passed the log consistency check.
	// but some of them may be stale, some of them may have conflict,
	// so I have to skip conflicts and append the actually new entries which I don't have.
	lastNewEntryIndex := r.appendNewEntries(prevLogIndex, m.Entries)

	// update commit index.
	r.tryUpdateCommitted(m.Commit, lastNewEntryIndex)

	r.forwardMsgUp(pb.Message{
		MsgType:   pb.MessageType_MsgAppendResponse,
		To:        m.From,
		From:      r.id,
		Term:      r.Term,
		Index:     lastNewEntryIndex, // used for leader to update the peer's Next. Only used in test suites.
		Reject:    false,
		NextIndex: lastNewEntryIndex + 1,
	})

	// it might take a long time to append and persist logs,
	// so reset timer again to make the system more robust.
	r.resetElectionTimer()
}

// handle AppendEntries RPC response.
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	r.Logger.recvAENTRes(m)

	// drop stale msgs.
	if m.Term < r.Term {
		return
	}

	// step down if I'm stale.
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	pr, ok := r.Prs[m.From]
	if !ok {
		return
	}

	oldNext := pr.Next
	oldMatch := pr.Match

	if m.Reject {
		switch m.Reason {
		case pb.RejectReason_IndexConflict:
			pr.Next = m.NextIndex
		case pb.RejectReason_TermConflict:
			// first search the log with the conflict term.
			l := r.RaftLog
			found := false
			for i := 1; i < int(l.Len()); i++ {
				// note, there's a dummy entry at the head.
				ent := l.entries[i]
				if ent.Term == m.ConflictTerm {
					found = true
					// the next set of entries sent to the follower will skip all logs with the conflict term.
					pr.Next = ent.Index + 1
					for _, ent = range l.entries[i+1:] {
						if ent.Index != m.ConflictTerm {
							pr.Next = ent.Index
							break
						}
					}
				}
			}

			if !found {
				// if not found, nothing to skip.
				// the only thing leader can do is to accept the suggestion from the server and try again.
				pr.Next = m.NextIndex
			}
		default:
			panic("unknown reject reason")
		}
		// to ensure the next index does not go below 1.
		pr.Next = max(pr.Next, 1)

	} else {
		// pr.Next = max(pr.Next, m.Index+1) // the test suites are creepy.
		pr.Next = max(pr.Next, m.NextIndex)
		pr.Match = pr.Next - 1
	}
	// next index cannot go beyond last index + 1.
	l := r.RaftLog
	pr.Next = min(pr.Next, l.LastIndex()+1)

	r.Logger.updateProgOf(m.From, oldNext, oldMatch, pr.Next, pr.Match)

	// if the next index is reduced, it means some conflicts occur and I immediately
	// retry to let the follower quickly keep in sync with me.
	if pr.Next < oldNext {
		r.sendAppendEntries(m.From, false)
	}

	// if a majority of followers has not rejected the entries, the entries are committed in the leader.
	if r.maybeUpdateCommitIndex() {
		// if the commit index is updated, immediately broadcast AppendEntries RPC to notify the followers ASAP.
		r.bcastAppendEntries(true)
	}
}

// broadcast Heartbeat RPC to all other peers in the cluster.
func (r *Raft) bcastHeartbeat() {
	r.Logger.bcastHBET()

	ids := r.idsFromPrs()
	for _, to := range ids {
		if to != r.id {
			// matchIndex is used for safety. It is a conservative measurement of what prefix of the log
			// the leader shares with a given follower.
			// it only gets updated when a follower positively acknowledges an AppendEntries RPC.
			// if the follower is slow or it recently joins the cluster or it was partitioned and then rejoined,
			// the follower's match index might be very small while the leader's commit index might be very large instead.
			// so to ensure that leader and followers make a consensus on the log entries, we cannot simply
			// pass leader's commit to the follower. Instead, we have to conservatively update the commit index
			// the same as we update the match index of this follower.
			l := r.RaftLog
			pr := r.Prs[to]
			commit := min(l.committed, pr.Match)

			r.forwardMsgUp(pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				To:      to,
				From:    r.id,
				Term:    r.Term,
				Commit:  commit,
			})
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.Logger.recvHBET(m)

	// drop stale msgs.
	if m.Term < r.Term {
		return
	}

	// I admit the from node is the current leader, so I step down and reset election timer
	// to not compete with him.
	r.becomeFollower(m.Term, m.From)
	r.resetElectionTimer()

	// update commit index.
	r.tryUpdateCommitted(m.Commit, r.RaftLog.LastIndex())

	// send the response.
	r.forwardMsgUp(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

// handle Heartbeat RPC response.
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// reject stale msgs.
	if m.Term < r.Term {
		return
	}

	// step down if I'm stale.
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	l := r.RaftLog
	pr, ok := r.Prs[m.From]
	if !ok {
		return
	}

	// now, I know this peer is alive but I don't know if it catches up.
	// if in my view, it does not catch up with me, I immediately send entries
	// I think it needs.
	if pr.Match < l.LastIndex() {
		r.sendAppendEntries(m.From, true)
	}
}

//
// utilities.
//

// transform []*pb.Entry to []pb.Entry since the appended log entries must be []pb.Entry.
func entsClone(ents []*pb.Entry) []pb.Entry {
	ents_clone := make([]pb.Entry, 0)
	for _, ent := range ents {
		ents_clone = append(ents_clone, *ent)
	}
	return ents_clone
}

// return a deep copy of the entry ent.
func entDeepCopy(ent pb.Entry) pb.Entry {
	ent_clone := pb.Entry{
		EntryType: ent.EntryType,
		Term:      ent.Term,
		Index:     ent.Index,
		Data:      ent.Data,
	}
	return ent_clone
}

// transform []pb.Entry to []*pb.Entry.
func entsRef(ents []pb.Entry) []*pb.Entry {
	ents_ref := make([]*pb.Entry, 0)
	// note, ent here is like a box. Each iteration we place a different element
	// into the box. But the &ent always return the address of the box, not the
	// address of the element in the box.
	// so becare of addressing in for loop in Go. Clone what you want to get address of .
	for _, ent := range ents {
		ent_clone := entDeepCopy(ent)
		ents_ref = append(ents_ref, &ent_clone)
	}
	return ents_ref
}

// return an array of peer ids. Note the array is sorted in ascending order.
func (r *Raft) idsFromPrs() []uint64 {
	ids := make([]uint64, 0)
	for id := range r.Prs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// the entries ents are appended whatsoever.
func (r *Raft) appendEntries(ents []*pb.Entry) {
	l := r.RaftLog
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
}

// append the actually new entries that I don't have.
// if all entries are stale or conflicting, return 0.
// otherwise, return the index of last new entry.
func (r *Raft) appendNewEntries(prevLogIndex uint64, nents []*pb.Entry) uint64 {
	l := r.RaftLog
	lastNewEntryIndex := prevLogIndex + uint64(len(nents))

	// since followers simply duplicate the leader's log,
	// and hence if an existing entry conflicts with the entries received from the leader,
	// delete the conflicting entry and all entries follow it in followers.
	for i := 0; i < len(nents); i++ {
		nent := nents[i]                // leader's entry.
		ent, err := l.Entry(nent.Index) // my entry.
		// all entris before the conflict entry were appended to my log.
		// all entries after and including the conflict entry needs to be appended to my log;
		if err != nil || ent.Term != nent.Term {
			if ent != nil && ent.Index <= l.committed {
				panic(ErrDiscardCommitted)
			}
			if ent != nil {
				// discard conflict entry and all follow it.
				offset := l.idx2off(ent.Index)
				r.Logger.discardEnts(l.entries[offset:])
				l.entries = l.entries[:offset]
				// some stable entries may be discarded, so update stable index.
				l.stabled = min(l.stabled, l.LastIndex())
			}

			// append new entries.
			nents_clone := entsClone(nents[i:])
			l.entries = append(l.entries, nents_clone...)

			r.Logger.appendEnts(nents_clone)

			return lastNewEntryIndex
		}
	}
	return lastNewEntryIndex
}

func (r *Raft) checkQuorumAppend(index uint64) bool {
	cnt := 1 // the leader has already replicated the log entry.
	for id, prs := range r.Prs {
		if id != r.id {
			if prs.Match >= index {
				cnt++
			}
		}
	}
	ok := 2*cnt > len(r.Prs)
	if ok {
		r.Logger.recvAppendQuorum(cnt)
	}
	return ok
}

func (r *Raft) maybeUpdateCommitIndex() bool {
	l := r.RaftLog
	for index := l.LastIndex(); index > l.committed; index-- {
		// only commit entries at the current term.
		ent, err := l.Entry(index)
		if err != nil || ent.Term != r.Term {
			continue
		}
		if r.checkQuorumAppend(index) {
			oldCommitted := l.committed
			l.committed = index
			r.Logger.updateCommitted(oldCommitted)
			return true
		}
	}
	return false
}

func (r *Raft) sendAppendEntries(to uint64, must bool) {
	l := r.RaftLog
	pr := r.Prs[to]

	prevLogIndex := l.lastIncludedIndex
	prevLogTerm := l.lastIncludedTerm
	prevLog, err := l.Entry(pr.Next - 1)
	if err == nil {
		prevLogIndex = prevLog.Index
		prevLogTerm = prevLog.Term
	}

	// entries to be sent.
	ents := l.sliceStartAt(pr.Next)
	if len(ents) == 0 && !must {
		return
	}
	ents_ref := entsRef(ents)

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  l.committed,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: ents_ref,
	}
	r.forwardMsgUp(m)

	r.Logger.sendEnts(prevLogIndex, prevLogTerm, ents, to)
}

// only used in test cases.
// I want a consistent naming convention in my implementation, so this method is only a wrapper
// of my implementation.
func (r *Raft) sendAppend(to uint64) bool {
	r.sendAppendEntries(to, true)
	return true
}

// update leader's own progress as test cases require.
func (r *Raft) updateLeaderProg() {
	l := r.RaftLog
	pr := r.Prs[r.id]

	oldNext := pr.Next
	oldMatch := pr.Match

	pr.Next = l.LastIndex() + 1
	// max is applied to follow the convention that match index never reduces.
	pr.Match = max(pr.Match, pr.Next-1)

	r.Logger.updateProgOf(r.id, oldNext, oldMatch, pr.Next, pr.Match)
}

func (r *Raft) tryUpdateCommitted(committed, lastNewEntryIndex uint64) {
	l := r.RaftLog
	if committed <= l.committed {
		return
	}

	oldCommitted := l.committed

	// committed entries must be those entries that I have
	l.committed = min(committed, lastNewEntryIndex)
	if l.committed < oldCommitted {
		panic("decrease commit index")
	}

	if l.committed != oldCommitted {
		r.Logger.updateCommitted(oldCommitted)
	}
}

func (r *Raft) tryUpdateApplied(applied uint64) {
	l := r.RaftLog
	if applied < l.applied || applied > l.committed {
		panic("invariant: applied <= committed and applied never decreases.")
	}
	// applied index never decreases.
	oldApplied := l.applied
	l.applied = applied
	r.Logger.updateApplied(oldApplied)
}

func (r *Raft) tryUpdateStabled(stabled uint64) {
	l := r.RaftLog
	// FIXME: Can stabled index decrease?
	oldStabled := l.stabled
	l.stabled = stabled
	r.Logger.updateStabled(oldStabled)
}
