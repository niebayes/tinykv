package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

var logger *Logger = makeLogger(false, "")

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// hints:
// (1) Each raft cmd is the data of a raft log entry. You shall use `msg.Marshal()` to marshall a raft cmd to []byte and wrap it to the data field
// of a raft log entry. Then propose the log entry to the raft module. When a log entry `ent` is applied, it shall be unmarshalled by calling `ent.Unmarshal()`,
// and then get processed by the state machine according to its command type.
// (2) Each raft cmd corresponds to one proposal. There defines a `proposals` field in `peer` struct and you shall
// interact with it both in `proposeRaftCommand` and `HandleRaftReady`. There also defines some helper functions you may need in `kv/raftstore/peer.go`.
// (3) Keep in mind to handle errors in `proposeRaftCommand` and `HandleRaftReady`. Refer to `kv/raftstore/util/errors.go` for what errors you shall
// handle, and refer to `kv/raftstore/cmd_resp.go` for how to bind these errors to `Resp` in `message.Callback`.
// (4) For this part and subsequent parts, an unreliable mock network is used as the testing environment. There might still have some flaws in your implementation
// even if you've passed one run of the tests. To ensure your implementation is correct, you shall run the tests multiple times. The great MIT 6.824 course has
// provided a [python script](https://blog.josejg.com/debugging-pretty/) for running multiple run of tests in parallel. It also provides a python script for prettifying
// your log output. You may need to learn the script and adjust your logging format to make the script works correctly.
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// check if we can propose this raft cmd.
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	if !d.IsLeader() {
		// I'm not the leader, but I will tell you which peer I think it's the leader.
		BindRespError(cb.Resp, &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		})
		cb.Done(cb.Resp)
		return
	}

	// FIXME: How to check if a raft cmd is stale or not? What raft cmds are regarded as stale cmds?
	if err = util.CheckTerm(msg, d.Term()); err != nil {
		BindRespError(cb.Resp, &util.ErrStaleCommand{})
		// FIXME: Shall I use NotifyStaleReq?
		cb.Done(cb.Resp)
		return
	}

	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	// record this proposal.
	prop := &proposal{
		// note, next proposal index = the last index of raft log + 1.
		// if a log entry is committed at index i, all proposals with indexes <= i shall be finished already.
		// if there're some proposals with indexes <= i not finished yet, they're regarded as stale cmds and
		// will not be executed.
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	d.proposals = append(d.proposals, prop)

	d.RaftGroup.Raft.Logger.NewProposal(msg, prop.index, prop.term)

	// propose this raft cmd to raft raw node.
	// note, even if this raft cmd contains multiple requests, it's also wrapped into one proposal.
	// this is because the raft module is responsible for replicating cmds, it does not execute cmds.
	// when the raft worker notifies this cmd is replicated successfully (i.e. committed, which will eventually be replicated on all servers),
	// it then executes the cmd by applying each request contained in the cmd to the state machine.
	err = d.RaftGroup.Propose(data)
	if err != nil {
		panic(err)
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
	if d.stopped {
		return
	}

	// fetch all ready states of the raft module.
	rn := d.peer.RaftGroup
	if !rn.HasReady() {
		return
	}
	rd := rn.Ready()

	// persist ready states.
	// TODO: handle applySnapResult.
	_, err := d.peerStorage.SaveReadyState(&rd, rn.Raft)
	if err != nil {
		panic(err)
	}

	// send pending raft msgs to other peers.
	d.Send(d.ctx.trans, rd.Messages)

	// execute raft cmds.
	for _, ent := range rd.CommittedEntries {
		// exit if the peer was stopped during executing the committed entries.
		if d.stopped {
			return
		}
		// skip already applied entry.
		if d.peerStorage.applyState.AppliedIndex >= ent.Index {
			continue
		}
		// each raft log entry corresponds to one raft cmd which may contain multiple requests.
		d.process(ent)
	}

	// FIXME: what if peer crashes right here?

	// notify the raft module that all ready state are processed.
	rn.Advance(rd)
}

func (d *peerMsgHandler) updateApplyState(index uint64, kvWB *engine_util.WriteBatch) {
	// advance and persist apply index.
	applyState := d.peerStorage.applyState
	applyState.AppliedIndex = max(applyState.AppliedIndex, index)
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
}

// since a no-op entry has no data, we only needs to advance the applied index.
func (d *peerMsgHandler) processNoopEntry(ent eraftpb.Entry) {
	kvWB := new(engine_util.WriteBatch)
	applyState := d.peerStorage.applyState
	oldAppliedIndex := applyState.AppliedIndex
	d.updateApplyState(ent.Index, kvWB)
	err := kvWB.WriteToDB(d.ctx.engine.Kv)
	if err != nil {
		panic(err)
	}
	// FIXME: Seems error occurs here.
	if oldAppliedIndex != applyState.AppliedIndex {
		d.RaftGroup.Raft.Logger.UpdateApplyState(oldAppliedIndex, applyState.AppliedIndex)
	}
	d.RaftGroup.Raft.Logger.ProcessedPropNoop(ent.Index)
}

func (d *peerMsgHandler) handleCompactLog(request *raft_cmdpb.CompactLogRequest, kvWB *engine_util.WriteBatch) {
	// update truncated state.
	applyState := d.peerStorage.applyState
	if applyState.TruncatedState.Index < request.CompactIndex {
		applyState.TruncatedState.Index = request.CompactIndex
		applyState.TruncatedState.Term = request.CompactTerm
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)

		// TODO: write to db and then schedule.

		d.ScheduleCompactLog(applyState.TruncatedState.Index)
		logger.ScheduleCompactLog(d.PeerId(), applyState.TruncatedState.Index, applyState.TruncatedState.Term)
	}
}

func (d *peerMsgHandler) handleChangePeer(request *raft_cmdpb.ChangePeerRequest) {
}

func (d *peerMsgHandler) handleTransferLeader(request *raft_cmdpb.TransferLeaderRequest) {
}

func (d *peerMsgHandler) handleSplit(request *raft_cmdpb.SplitRequest) {
}

func (d *peerMsgHandler) handleAdminRequest(request *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	switch request.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.handleCompactLog(request.CompactLog, kvWB)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.handleChangePeer(request.ChangePeer)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.handleTransferLeader(request.TransferLeader)
	case raft_cmdpb.AdminCmdType_Split:
		d.handleSplit(request.Split)
	default:
		panic("invalid admin request type")
	}
}

func (d *peerMsgHandler) processRaftCommand(ent eraftpb.Entry, cmd *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, bool) {
	kvWB := new(engine_util.WriteBatch)
	cmd_resp := newCmdResp()

	if ent.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := eraftpb.ConfChange{}
		if err := proto.Unmarshal(ent.Data, &cc); err != nil {
			panic(err)
		}
		// TODO: persist conf state.
		// FIXME: How to persist conf state?
		d.RaftGroup.ApplyConfChange(cc)
	}

	needNewTxn := false
	for _, request := range cmd.Requests {
		if d.handleClientRequest(request, cmd_resp, kvWB) {
			needNewTxn = true
		}
	}

	applyState := d.peerStorage.applyState
	oldTruncatedIndex := applyState.TruncatedState.Index
	oldTruncatedTerm := applyState.TruncatedState.Term

	if cmd.AdminRequest != nil {
		d.handleAdminRequest(cmd.AdminRequest, kvWB)
	}

	oldAppliedIndex := applyState.AppliedIndex

	// advance and persist apply index.
	d.updateApplyState(ent.Index, kvWB)

	// apply the batch of writes.
	err := kvWB.WriteToDB(d.ctx.engine.Kv)
	if err != nil {
		panic(err)
	}

	if oldTruncatedIndex != applyState.TruncatedState.Index {
		logger.UpdateTruncatedState(d.PeerId(), oldTruncatedIndex, oldTruncatedTerm,
			applyState.TruncatedState.Index, applyState.TruncatedState.Term)
	}

	// TODO: replace raft.logger with my logger.
	if oldAppliedIndex != applyState.AppliedIndex {
		d.RaftGroup.Raft.Logger.UpdateApplyState(oldAppliedIndex, applyState.AppliedIndex)
	}

	d.RaftGroup.Raft.Logger.ProcessedProp(ent.Index)

	return cmd_resp, needNewTxn
}

func (d *peerMsgHandler) processNonLeader(ent eraftpb.Entry, cmd *raft_cmdpb.RaftCmdRequest) {
	d.processRaftCommand(ent, cmd)
}

func (d *peerMsgHandler) processLeader(ent eraftpb.Entry, cmd *raft_cmdpb.RaftCmdRequest) {
	// A scenario worth noting:
	// if the log entry corresponding to a raft cmd is committed, it will eventually be committed by
	// all alive peers. Assume the old leader crashes or is partitioned after committing this log
	// entry, the new elected leader must be one of the servers that has been already appended this
	// log entry, since the vote restriction says only the server has the most up-to-date log will
	// become the new leader. Even if the new leader does not aware of this log entris was committed,
	// upon becoming the leader, this server will append and commit a no-op entry, and as a result
	// all preceding uncommitted entries will be committed.

	// Another scenario worth noting:
	// assume a client sends a Put request following a Get request on the same key.
	// if the current leader crashes or is partitioned after committing and responding the log entry
	// corresponding to the Put request, the subsequent Get request would still get the expected
	// value. This is because if the log entry corresponding to the Put request is committed, it will
	// eventually be executed by all alive peers which certainly includes the new leader. And each
	// peer would execute log entries in order, and hence before the new leader responding the Get
	// request, it must have been executed the Put request.

	// FIXME: How does tinykv handles this problem?
	// if the leader crashes after committing the log entry but before responding to the client,
	// the client will retry the command with a new leader, causing it to be executed a second time.
	// The solution is for clients to assign unique serial numbers to every command. Then, the state
	// machine tracks the latest serial number processed for each client, along with the associated
	// response. If it receives a command whose serial number has already been executed, it responds
	// immediately without re-executing the request.

	// if this raft cmd contains at least one snap request, then we need to new a txn.
	cmd_resp, needNewTxn := d.processRaftCommand(ent, cmd)

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
	// TODO: ensure the proposal truncation is correct.
	if update {
		newi := stale_index + 1
		if newi >= len(d.proposals) {
			d.proposals = d.proposals[:0]
		} else {
			d.proposals = d.proposals[newi:]
		}
	}
}

// process a log entry.
func (d *peerMsgHandler) process(ent eraftpb.Entry) {
	if len(ent.Data) == 0 {
		d.processNoopEntry(ent)
		return
	}

	cmd := &raft_cmdpb.RaftCmdRequest{}
	err := proto.Unmarshal(ent.Data, cmd)
	if err != nil {
		panic(err)
	}

	// leader and non-leaders has different behaviors on processing a raft cmd.
	if d.IsLeader() {
		d.processLeader(ent, cmd)
	} else {
		d.processNonLeader(ent, cmd)
	}
}

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
		// l := makeLogger(false, "")
		// l.GetResponse(d.PeerId(), p.index, request.Get.Key, resp.Value)
		// log.Infof("N%v processed Get request: key %v \n", d.PeerId(), string(request.Get.Key))

	case raft_cmdpb.CmdType_Put:
		resp := d.handlePutRequest(request, kvWB)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     resp,
		})
		// log.Infof("N%v processed Put request: key: %v val: %v \n", d.PeerId(), string(request.Put.Key), string(request.Put.Value))

	case raft_cmdpb.CmdType_Delete:
		resp := d.handleDeleteRequest(request, kvWB)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  resp,
		})
		// log.Infof("N%v processed Delete request: key %v \n", d.PeerId(), string(request.Delete.Key))

	case raft_cmdpb.CmdType_Snap:
		resp := d.handleSnapRequest(request)
		cmd_resp.Responses = append(cmd_resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    resp,
		})
		// when a client wishes to read the kv, the storage returns it a snapshot which is wrapped into
		// a txn. So, we start a new txn here, and let the service handler discards the txn.
		needNewTxn = true
		// log.Infof("N%v processed Snap request \n", d.PeerId())

	default:
		panic("unknown cmd type")
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

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
