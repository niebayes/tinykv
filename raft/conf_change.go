package raft

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// FIXME: What shall I do if the node already exists?
	_, ok := r.Prs[id]
	// FIXME: Shall always reset progress?
	if !ok {
		r.Prs[id] = r.newProgress()

		r.Logger.addNode(id)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// FIXME: What Shall I do if the node does exist?
	for i := range r.Prs {
		if i == id {
			delete(r.Prs, i)
			delete(r.votes, i)
			r.maybeUpdateCommitIndex()

			r.Logger.removeNode(id)
		}
	}
}
