//
// this file contains methods related to local message handling.
//
// there're 3 type of local messages: MsgHup, MsgBeat and MsgPropose.
// for what purpose each message has, please refers to `doc.go`
//

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handle MsgHup message
func (r *Raft) handleMsgHup(msg pb.Message) {
	r.becomeCandidate()
	// if the cluster only contain one node, this node immediately becomes the leader.
	if len(r.peers) == 1 {
		r.becomeLeader()
		return
	}
	// otherwise, there must be a majority of votes from the cluster to become the leader.
	r.bcastRequestVote()
}

// handle MsgBeat message.
func (r *Raft) handleBeat(msg pb.Message) {
	r.bcastHeartbeat()
}
