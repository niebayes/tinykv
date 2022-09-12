package raftstore

import (
	// "time"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// reject raft cmd if this peer is stopped.
	if d.stopped {
		return
	}

	// check if we can propose this raft cmd, i.e.
	// check region epoch, check leadership, check stale cmd by comparing terms.
	if err := d.preProposeRaftCommand(msg); err != nil {
		callbackDoneWithErr(cb, err)
		return
	}

	// if the key is not in the region, drop these client requests.
	for _, request := range msg.Requests {
		key := getKey(request)
		if key != nil {
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				callbackDoneWithErr(cb, err)
				return
			}
		}
	}

	// if the split key is not in the region, drop this split request
	if isSplitAdmin(msg) {
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
			callbackDoneWithErr(cb, err)
			return
		}
	}

	// since there's no need to do replication on a TransferLeader cmd, we directly execute it right here.
	// note, TransferLeader can be sent to a non-leader.
	if isTransferLeaderAdmin(msg) {
		// logger.RecvAdmin(d.PeerId(), msg.AdminRequest)

		d.handleTransferLeader(msg.AdminRequest.TransferLeader)
		// seems TransferLeader would not convey a non-nil callback. But I choose to conservatively reply
		// each request as long as the callback is not nil.
		if cb != nil {
			cmd_resp := newCmdResp()
			cmd_resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(cmd_resp)
		}
		return
	}

	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	nextPropIndex := d.nextProposalIndex()

	// only one pending config change is allowed.
	if isConfChangeAdmin(msg) {
		r := d.RaftGroup.Raft
		if r.PendingConfIndex == raft.None {
			r.PendingConfIndex = nextPropIndex
		} else if r.PendingConfIndex < d.peerStorage.AppliedIndex() {
			// reject this config change request if there's a pending
			// config change waiting to be applied.
			return
		}
	}

	// record this proposal.
	prop := &proposal{
		// note, next proposal index = the last index of raft log + 1.
		// if a log entry is committed at index i, all proposals with indexes <= i shall be finished already.
		// if there're some proposals with indexes <= i not finished yet, they're regarded as stale cmds and
		// will not be executed.
		index: nextPropIndex,
		term:  d.Term(),
		cb:    cb,
	}
	d.proposals = append(d.proposals, prop)

	// TODO: elaborate the proposal by adding admin type.
	d.RaftGroup.Raft.Logger.NewProposal(msg, prop.index, prop.term)

	// propose this raft cmd to raft raw node.
	// note, even if this raft cmd contains multiple requests, it's also wrapped into one proposal.
	// this is because the raft module is responsible for replicating cmds, it does not execute cmds.
	// when the raft worker notifies this cmd is replicated successfully (i.e. committed, which will eventually be replicated on all servers),
	// it then executes the cmd by applying each request contained in the cmd to the state machine.

	if msg.AdminRequest != nil {
		logger.RecvAdmin(d.PeerId(), msg.AdminRequest)
	}

	if isConfChangeAdmin(msg) {
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		cc := eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    context,
		}
		if err = d.RaftGroup.ProposeConfChange(cc); err != nil {
			panic(err)
		}

	} else {
		if err = d.RaftGroup.Propose(data); err != nil {
			panic(err)
		}
	}
}

// HandleRaftReady should get the ready from Raft module and do corresponding actions like
// persisting log entries, applying committed entries and sending raft messages to other peers through the network.
// TODO: Add raft invariant checker after each change.
// 	for e.g. applied index must increase continuously.
//					 commit index >= applied index
//					 commit index <= stabled index
//					 valid state transitions: i.e. only in certain scenarios, could a server change from one state to another.
//					 cannot discard committed entries.
//					 executed log entry must have index > applied index, i.e. cannot execute one stale entry.
// TODO: Add safety checker for each safety property described in raft paper.
func (d *peerMsgHandler) HandleRaftReady() {
	// stop handling raft ready if I am stopped.
	if d.stopped {
		return
	}

	// fetch all ready states of the raft module.
	rn := d.peer.RaftGroup
	if !rn.HasReady() {
		return
	}
	rd := rn.Ready()

	// persist ready states and install snapshot.
	applySnapResult := d.peerStorage.SaveReadyState(&rd, rn.Raft)
	if applySnapResult != nil {
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		region := applySnapResult.Region
		storeMeta.regions[region.Id] = region
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		d.peerStorage.SetRegion(region)
		storeMeta.Unlock()
	}

	// send pending raft msgs to other peers.
	d.Send(d.ctx.trans, rd.Messages)

	// execute committed raft cmds.
	for _, ent := range rd.CommittedEntries {
		// exit if the peer was stopped during executing committed entries.
		if d.stopped {
			return
		}
		// skip applied entry.
		if d.peerStorage.applyState.AppliedIndex >= ent.Index {
			continue
		}
		// each raft log entry corresponds to one raft cmd which may contain multiple requests.
		d.processLogEntry(ent)
	}

	// do not advance raft state if stopped.
	if !d.stopped {
		// notify the raft module that all ready state are handled.
		rn.Advance(rd)
	}
}

// process a committed log entry.
func (d *peerMsgHandler) processLogEntry(ent eraftpb.Entry) {
	if len(ent.Data) == 0 {
		d.processNoopEntry(ent)
		return
	}
	// A scenario worth noting:
	// if the log entry corresponding to a raft cmd is committed, it will eventually be committed by
	// all alive peers. Assume the old leader crashes or is partitioned after committing this log
	// entry, the new elected leader must be one of the servers that has been already appended this
	// log entry, since the vote restriction says only the server has the most up-to-date log will
	// become the new leader. Even if the new leader does not aware of this log entris was committed,
	// upon becoming the leader, this server will append and commit a no-op entry, and as a result
	// all preceding uncommitted entries will be committed.
	//
	// Another scenario worth noting:
	// assume a client sends a Put request following a Get request on the same key.
	// if the current leader crashes or is partitioned after committing and responding the log entry
	// corresponding to the Put request, the subsequent Get request would still get the expected
	// value. This is because if the log entry corresponding to the Put request is committed, it will
	// eventually be executed by all alive peers which certainly includes the new leader. And each
	// peer would execute log entries in order, and hence before the new leader responding the Get
	// request, it must have been executed the Put request.
	//
	// FIXME: How does tinykv handles this problem?
	// if the leader crashes after committing the log entry but before responding to the client,
	// the client will retry the command with a new leader, causing it to be executed a second time.
	// The solution is for clients to assign unique serial numbers to every command. Then, the state
	// machine tracks the latest serial number processed for each client, along with the associated
	// response. If it receives a command whose serial number has already been executed, it responds
	// immediately without re-executing the request.

	// FIXME: small granularity of write batch or large granularity? Large for grouping updates of related data in one batch
	// and to ensure crash consistency.

	// record old raft state for logging.
	applyState := d.peerStorage.applyState
	oldTruncatedIndex := applyState.TruncatedState.Index
	oldTruncatedTerm := applyState.TruncatedState.Term
	oldAppliedIndex := applyState.AppliedIndex

	cmd_resp := newCmdResp()

	if ent.EntryType == eraftpb.EntryType_EntryConfChange {
		d.handleChangePeer(ent, cmd_resp)
		// since this peer may be destroy due to processing a change peer cmd, this peer shall not
		// continue processing.
		if d.stopped {
			return
		}
	}

	needNewTxn := false

	if ent.EntryType == eraftpb.EntryType_EntryNormal {
		cmd := &raft_cmdpb.RaftCmdRequest{}
		if err := cmd.Unmarshal(ent.Data); err != nil {
			panic(err)
		}

		// handle client requests.
		kvWB := new(engine_util.WriteBatch)
		for _, request := range cmd.Requests {
			key := getKey(request)
			if key != nil {
				if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
					BindRespError(cmd_resp, err)
					break
				}
			}
			if d.handleClientRequest(request, cmd_resp, kvWB) {
				needNewTxn = true
			}
		}
		if cmd_resp.Header.Error == nil && kvWB.Len() > 0 {
			kvWB.MustWriteToDB(d.ctx.engine.Kv)
		}

		// handle admin requests.
		if cmd.AdminRequest != nil {
			switch cmd.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				d.handleCompactLog(cmd.AdminRequest.CompactLog)
			case raft_cmdpb.AdminCmdType_Split:
				d.handleSplit(cmd, cmd_resp)
			case raft_cmdpb.AdminCmdType_TransferLeader, raft_cmdpb.AdminCmdType_ChangePeer:
				panic("shall not reach here")
			default:
				panic("unknown admin request")
			}
		}
	}

	// advance and persist apply index.
	d.updateApplyState(ent.Index)

	if oldTruncatedIndex != applyState.TruncatedState.Index {
		logger.UpdateTruncatedState(d.PeerId(), oldTruncatedIndex, oldTruncatedTerm,
			applyState.TruncatedState.Index, applyState.TruncatedState.Term)
	}

	// TODO: replace raft.logger with my logger.
	if oldAppliedIndex != applyState.AppliedIndex {
		d.RaftGroup.Raft.Logger.UpdateApplyState(oldAppliedIndex, applyState.AppliedIndex)
	}

	d.RaftGroup.Raft.Logger.ProcessedProp(ent.Index)

	// the leader is responsible for replying the client and delete proposal.
	if d.IsLeader() {
		d.finishProposal(ent, cmd_resp, needNewTxn)
	}
}

func (d *peerMsgHandler) finishProposal(ent eraftpb.Entry, cmd_resp *raft_cmdpb.RaftCmdResponse, needNewTxn bool) {
	// find the corresponding proposal of this log entry.
	stale_index := 0 // the start index of the proposals array after processing.
	update := false  // do I need to update proposal array?
	for i := 0; !d.stopped && i < len(d.proposals); i++ {
		p := d.proposals[i]
		// FIXME: Is my stale check incorrect?
		// TODO: modify the check logic.
		if p.index < ent.Index || p.term < ent.Term {
			if p.cb != nil {
				NotifyStaleReq(d.Term(), p.cb)
				d.RaftGroup.Raft.Logger.NotifyStaleProp(p.index, p.term, ent.Index, ent.Term)
			}
			stale_index = i
			update = true

		} else if p.index == ent.Index && p.term == ent.Term {
			// notify the client.
			// note, although a raft cmd may contain multiple requests, but there cannot be multiple
			// snap requests in one raft cmd. This is because a snap request is only issued when the Reader
			// interface is called on the Storage interface. And one Reader call only issue a single
			// snap request, and no more other requests.
			// so we only need to bind the new txn on the raft cmd, rather than on each snap request.
			if p.cb != nil {
				if needNewTxn {
					p.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
				}
				p.cb.Done(cmd_resp)
				d.RaftGroup.Raft.Logger.NotifyClient(p.index)
			}

			// this cmd was processed and hence it's also marked staled.
			stale_index = i
			update = true
			break
		}
	}

	// the start index of the remaining proposals after discarding stale and processed proposals.
	if update {
		newi := stale_index + 1
		if newi >= len(d.proposals) {
			d.proposals = d.proposals[:0]
		} else {
			d.proposals = d.proposals[newi:]
		}
	}

}

//
// admin requests handler.
//

func (d *peerMsgHandler) handleCompactLog(request *raft_cmdpb.CompactLogRequest) {
	applyState := d.peerStorage.applyState
	// update truncated state.
	// FIXME: Is this really possible to happen?
	// note, if CompactIndex == TruncatedState.Index, we still need to update truncated term
	// since we never reject this log compaction request and truncated term may be overwritten.
	if applyState.TruncatedState.Index <= request.CompactIndex {
		applyState.TruncatedState.Index = request.CompactIndex
		applyState.TruncatedState.Term = request.CompactTerm

		// we have to persist the apply state and hence schedule a compact log task.
		// if we do not persist the apply state first, then:
		// assume the machine crashes at the moment that the compact log task is finised
		// while the apply state is not persisted yet. Then upon recovery, the log is compacted
		// but the apply state is the stale one, and hence there's an inconsistency.
		kvWB := new(engine_util.WriteBatch)
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
		kvWB.MustWriteToDB(d.ctx.engine.Kv)

		d.ScheduleCompactLog(applyState.TruncatedState.Index)
		// TODO: add logging for the begin and end of log compaction.
		logger.ScheduleCompactLog(d.PeerId(), applyState.TruncatedState.Index, applyState.TruncatedState.Term)
	}
}

func (d *peerMsgHandler) handleTransferLeader(request *raft_cmdpb.TransferLeaderRequest) {
	d.RaftGroup.TransferLeader(request.Peer.Id)
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) handleChangePeer(ent eraftpb.Entry, cmd_resp *raft_cmdpb.RaftCmdResponse) {
	cc := eraftpb.ConfChange{}
	if err := cc.Unmarshal(ent.Data); err != nil {
		panic(err)
	}

	cmd := raft_cmdpb.RaftCmdRequest{}
	if err := cmd.Unmarshal(cc.Context); err != nil {
		panic(err)
	}

	err := util.CheckRegionEpoch(&cmd, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		BindRespError(cmd_resp, errEpochNotMatching)
		return
	}

	kvWB := new(engine_util.WriteBatch)

	request := cmd.AdminRequest.ChangePeer
	switch request.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// check the existence of peer to de-duplicate config change cmds.
		region := d.Region()
		if isPeerInRegion(request.Peer.Id, request.Peer.StoreId, region) {
			return
		}

		// TODO: elaborate this process.
		// the next round of heartbeat will try to access the new added peer,
		// and the store worker would try to create a peer instance for this peer
		// by calling `maybeCreatePeer`. If this call succeeds, the new added peer
		// will start its work.

		// for logging.
		oldConfVer := region.RegionEpoch.ConfVer
		oldVer := region.RegionEpoch.Version

		region.RegionEpoch.ConfVer++
		region.Peers = append(region.Peers, request.Peer)
		d.insertPeerCache(request.Peer)

		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		kvWB.MustWriteToDB(d.ctx.engine.Kv)

		log.Infof("region state key: %v", meta.RegionStateKey(region.Id))
		logger.AddPeer(d.PeerId(), request.Peer.Id)
		logger.UpdateEpoch(d.PeerId(), oldConfVer, oldVer, region.RegionEpoch)

	case eraftpb.ConfChangeType_RemoveNode:
		// ensure the peer to be removed is in the region currently.
		region := d.Region()
		if !isPeerInRegion(request.Peer.Id, request.Peer.StoreId, region) {
			return
		}

		// the node to be removed is me.
		if request.Peer.Id == d.PeerId() && request.Peer.StoreId == d.storeID() {
			// note, storeMeta will be updated inside destroyPeer.
			// TODO: properly handle the case I'm the leader.
			d.destroyPeer()

			logger.DestroyPeer(d.PeerId(), request.Peer.Id)

		} else {
			oldConfVer := region.RegionEpoch.ConfVer
			oldVer := region.RegionEpoch.Version

			region.RegionEpoch.ConfVer++
			newPeers := make([]*metapb.Peer, 0)
			for _, p := range region.Peers {
				if p.Id != request.Peer.Id || p.StoreId != request.Peer.StoreId {
					newPeers = append(newPeers, p)
				}
			}
			region.Peers = newPeers
			d.removePeerCache(request.Peer.Id)

			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			kvWB.MustWriteToDB(d.ctx.engine.Kv)

			log.Infof("region state key: %v", meta.RegionStateKey(region.Id))
			logger.RemovePeer(d.PeerId(), request.Peer.Id)
			logger.UpdateEpoch(d.PeerId(), oldConfVer, oldVer, region.RegionEpoch)
		}

	default:
		panic("unknown change type")
	}

	d.RaftGroup.ApplyConfChange(cc)

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}

	cmd_resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{
			Region: d.Region(),
		},
	}
}

func (d *peerMsgHandler) handleSplit(cmd_req *raft_cmdpb.RaftCmdRequest, cmd_resp *raft_cmdpb.RaftCmdResponse) {
	// TODO: add logging for the begin and end of a split.

	if !d.checkSplitCondition(cmd_req, cmd_resp) {
		return
	}

	region := d.Region()
	request := cmd_req.AdminRequest.Split
	kvWB := new(engine_util.WriteBatch)

	// create the new region. The Peers field in region are the metadata of peers in the region.
	newRegion := d.createRegion(request.NewRegionId, request.NewPeerIds, request.SplitKey, region.EndKey)

	// since we are about to change the key range of the old region, and store metadata maintains
	// a regionRanges to track the key range of each region, so we choose to lock store metadata
	// starting from here.
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()

	// update region metadata of the old region.
	region.EndKey = request.SplitKey
	if engine_util.ExceedEndKey(region.StartKey, region.EndKey) {
		panic("start key goes over end key")
	}

	oldConfVer := region.RegionEpoch.ConfVer
	oldVer := region.RegionEpoch.Version

	region.RegionEpoch.Version++

	// add in store metadata of the new region.
	storeMeta.regions[newRegion.Id] = newRegion
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})

	storeMeta.Unlock()

	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

	// TODO: rewrite logger by taking region id into account.

	log.Infof("N%v R%v old region state key: %v", d.PeerId(), region.Id, meta.RegionStateKey(region.Id))
	log.Infof("N%v R%v new region state key: %v", d.PeerId(), newRegion.Id, meta.RegionStateKey(newRegion.Id))

	// change on region metadata and store metadata must be persisted before we create the peer.
	kvWB.MustWriteToDB(d.ctx.engine.Kv)

	// creata the peer responsible for the new region in this store. Peers for the new region in other stores
	// will be created when they execute the same split request.
	peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		panic(err)
	}

	logger.RegionSplit(d.PeerId(), newRegion, peer)

	// register the peer to router.
	d.ctx.router.register(peer)

	// send a MsgTypeStart to the peer. Upon receiving, the peer will start ticker and all its work starts.
	if err = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart}); err != nil {
		panic(err)
	}

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}

	cmd_resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{
				region, newRegion,
			},
		},
	}

	logger.UpdateEpoch(d.PeerId(), oldConfVer, oldVer, region.RegionEpoch)
}

//
// client requests handler.
//

func (d *peerMsgHandler) handleClientRequest(request *raft_cmdpb.Request, cmd_resp *raft_cmdpb.RaftCmdResponse, kvWB *engine_util.WriteBatch) bool {
	needNewTxn := false

	switch request.CmdType {
	case raft_cmdpb.CmdType_Get:
		// FIXME: Does tinykv implement this mechanism? If yes, where?
		// a leader must check whether it has been deposed before processing a read-only request
		// (its information may be stale if a more recent leader has been elected).
		// Raft handles this by having the leader exchange heartbeat messages
		// with a majority of the cluster before responding to read-only requests.
		resp := d.handleGetRequest(request)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     resp,
		})

	case raft_cmdpb.CmdType_Put:
		resp := d.handlePutRequest(request, kvWB)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     resp,
		})

	case raft_cmdpb.CmdType_Delete:
		resp := d.handleDeleteRequest(request, kvWB)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  resp,
		})

	case raft_cmdpb.CmdType_Snap:
		resp := d.handleSnapRequest(request)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    resp,
		})
		// when a client wishes to read the kv, the storage returns it a snapshot which is wrapped into
		// a txn. So, we start a new txn here, and let the service handler discards the txn.
		needNewTxn = true

	default:
		panic("unknown client request")
	}
	return needNewTxn
}

func (d *peerMsgHandler) handleGetRequest(request *raft_cmdpb.Request) *raft_cmdpb.GetResponse {
	val, err := engine_util.GetCF(d.ctx.engine.Kv, request.Get.Cf, request.Get.Key)
	response := &raft_cmdpb.GetResponse{}
	if err == nil {
		response.Value = val
	}
	return response
}

func (d *peerMsgHandler) handlePutRequest(request *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) *raft_cmdpb.PutResponse {
	kvWB.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
	response := &raft_cmdpb.PutResponse{}
	return response
}

func (d *peerMsgHandler) handleDeleteRequest(request *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) *raft_cmdpb.DeleteResponse {
	kvWB.DeleteCF(request.Delete.Cf, request.Delete.Key)
	response := &raft_cmdpb.DeleteResponse{}
	return response
}

func (d *peerMsgHandler) handleSnapRequest(request *raft_cmdpb.Request) *raft_cmdpb.SnapResponse {
	response := &raft_cmdpb.SnapResponse{
		Region: d.Region(),
	}
	return response
}

//
// utlities
//

func isConfChangeAdmin(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest != nil && msg.AdminRequest.CmdType ==
		raft_cmdpb.AdminCmdType_ChangePeer
}

func isTransferLeaderAdmin(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest != nil && msg.AdminRequest.CmdType ==
		raft_cmdpb.AdminCmdType_TransferLeader
}

func isSplitAdmin(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest != nil && msg.AdminRequest.CmdType ==
		raft_cmdpb.AdminCmdType_Split
}

func callbackDoneWithErr(cb *message.Callback, err error) {
	if cb != nil {
		cb.Done(ErrResp(err))
	}
}

// since a no-op entry has no data, we only needs to advance the applied index.
func (d *peerMsgHandler) processNoopEntry(ent eraftpb.Entry) {
	applyState := d.peerStorage.applyState
	oldAppliedIndex := applyState.AppliedIndex
	d.updateApplyState(ent.Index)
	// FIXME: Seems error occurs here.
	if oldAppliedIndex != applyState.AppliedIndex {
		d.RaftGroup.Raft.Logger.UpdateApplyState(oldAppliedIndex, applyState.AppliedIndex)
	}
	d.RaftGroup.Raft.Logger.ProcessedPropNoop(ent.Index)
}

func (d *peerMsgHandler) updateApplyState(index uint64) {
	// advance and persist apply index.
	applyState := d.peerStorage.applyState
	applyState.AppliedIndex = max(applyState.AppliedIndex, index)
	kvWB := new(engine_util.WriteBatch)
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
	kvWB.MustWriteToDB(d.ctx.engine.Kv)
}

func (d *peerMsgHandler) createRegion(id uint64, peer_ids []uint64, startKey, endKey []byte) *metapb.Region {
	if engine_util.ExceedEndKey(startKey, endKey) {
		panic("start key goes over end key")
	}

	newRegion := &metapb.Region{
		Id:       id,
		StartKey: startKey,
		EndKey:   endKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: InitEpochConfVer,
			Version: InitEpochVer,
		},
		Peers: make([]*metapb.Peer, len(peer_ids)),
	}
	for i := 0; i < len(peer_ids); i++ {
		newRegion.Peers[i] = &metapb.Peer{
			Id:      peer_ids[i],
			StoreId: d.Region().Peers[i].StoreId,
		}
	}
	return newRegion
}

func getKey(request *raft_cmdpb.Request) []byte {
	switch request.CmdType {
	case raft_cmdpb.CmdType_Get:
		return request.Get.Key
	case raft_cmdpb.CmdType_Put:
		return request.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return request.Delete.Key
	case raft_cmdpb.CmdType_Snap:
		// ignore.
	default:
		panic("unknown client request")
	}
	return nil
}

func isPeerInRegion(peerId, peerStoreId uint64, region *metapb.Region) bool {
	for _, p := range region.Peers {
		if p.Id == peerId && p.StoreId == peerStoreId {
			return true
		}
	}
	return false
}

// check if this split request is reasonable.
func (d *peerMsgHandler) checkSplitCondition(cmd_req *raft_cmdpb.RaftCmdRequest, cmd_resp *raft_cmdpb.RaftCmdResponse) bool {
	region := d.Region()
	request := cmd_req.AdminRequest.Split

	// the region this peer responsible for is changed.
	if cmd_req.Header.RegionId != region.Id {
		BindRespError(cmd_resp, &util.ErrRegionNotFound{})
		return false
	}

	if request.NewRegionId == region.Id {
		panic("new region id == region id")
	}

	// validate the request is not stale due to region epoch change.
	// region epoch contains conf ver and version which may be changed due to
	// conf change and region split/merge. If any happened, then the key range or peer
	// config may be changed and hence this request is regarded stale and hence shall be rejected.
	err := util.CheckRegionEpoch(cmd_req, region, true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		BindRespError(cmd_resp, errEpochNotMatching)
		return false
	}

	// validate the request is not stale due to peers change.
	// in AskSplit, the scheduler assumes that the new region initially has the same #peers as the
	// old region, so it allocates exactly #peer ids as the #peers.
	// if however the region was splitted or mergerd before applying this split request, then this split
	// request is regarded stale and hence shall be ignored.
	if len(region.Peers) != len(request.NewPeerIds) {
		BindRespError(cmd_resp, &util.ErrStaleCommand{})
		return false
	}

	// ensure the split key is still in my key range.
	if err := util.CheckKeyInRegion(request.SplitKey, region); err != nil {
		BindRespError(cmd_resp, err)
		return false
	}

	return true
}
