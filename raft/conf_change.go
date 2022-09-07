package raft

// TODO: install these hints.
// 只对还在共识组配置中的 raftnode 进行 tick。
// 新当选的 leader 需要保证之前任期的所有 log 都被 apply 后才能进行新的 conf change 变更，这有关 raft 单步配置变更的 safety，可以参照 邮件 和相关 博客。
// 只有当前共识组的最新配置变更日志被 apply 后才可以接收新的配置变更日志。
// 增删节点时需要维护 PeerTracker。


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
