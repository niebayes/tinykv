package raftstore

import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
)

func isRangeEmpty(engine *badger.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	err := engine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(startKey)
		if it.Valid() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) < 0 {
				hasData = true
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	return !hasData, err
}

// ensure the kvdb and raftdb are all empty initially.
// associate the global var meta.StoreIdentKey with a StoreIdent struct, so that upon restarting,
// the cluster id and store id can be recoverd using the global var (its value is static).
func BootstrapStore(engines *engine_util.Engines, clusterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isRangeEmpty(engines.Kv, meta.MinKey, meta.MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and has already had data.")
	}
	empty, err = isRangeEmpty(engines.Raft, meta.MinKey, meta.MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clusterID
	ident.StoreId = storeID
	// note, meta.StoreIdentKey is a global var.
	err = engine_util.PutMeta(engines.Kv, meta.StoreIdentKey, ident)
	if err != nil {
		return err
	}
	return nil
}

func PrepareBootstrap(engines *engine_util.Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte{},
		EndKey:   []byte{},
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: []*metapb.Peer{
			{
				Id:      peerID,
				StoreId: storeID,
			},
		},
	}
	err := PrepareBootstrapCluster(engines, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

// persist some metadata so that upon restarting, these metadata could be restored using the corresponding
// global vars (which are used as the keys in the db).
// write initial RegionLocalState to kvdb.
// write initial RaftApplyState to raftdb.
// write initial RaftLocalState to raftdb.
//
// RaftLocalState: Used to store HardState of the current Raft and the last Log Index.
// RaftApplyState: Used to store the last Log index that Raft applies and some truncated Log information.
// RegionLocalState: Used to store Region information and the corresponding Peer state on this Store.
//			Normal indicates that this Peer is normal.
//      Tombstone shows that this Peer has been removed from Region and cannot join in Raft Group.
func PrepareBootstrapCluster(engines *engine_util.Engines, region *metapb.Region) error {
	// persist RegionLocalState.
	state := new(rspb.RegionLocalState)
	state.Region = region
	kvWB := new(engine_util.WriteBatch)
	kvWB.SetMeta(meta.PrepareBootstrapKey, state)
	kvWB.SetMeta(meta.RegionStateKey(region.Id), state)

	// persist RaftApplyState.
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}

	// persist RaftLocalState.
	raftWB := new(engine_util.WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return nil
}

func writeInitialApplyState(kvWB *engine_util.WriteBatch, regionID uint64) {
	applyState := &rspb.RaftApplyState{
		AppliedIndex: meta.RaftInitLogIndex,
		TruncatedState: &rspb.RaftTruncatedState{
			Index: meta.RaftInitLogIndex,
			Term:  meta.RaftInitLogTerm,
		},
	}
	kvWB.SetMeta(meta.ApplyStateKey(regionID), applyState)
}

func writeInitialRaftState(raftWB *engine_util.WriteBatch, regionID uint64) {
	raftState := &rspb.RaftLocalState{
		HardState: &eraftpb.HardState{
			Term:   meta.RaftInitLogTerm,
			Commit: meta.RaftInitLogIndex,
		},
		LastIndex: meta.RaftInitLogIndex,
	}
	raftWB.SetMeta(meta.RaftStateKey(regionID), raftState)
}

func ClearPrepareBootstrap(engines *engine_util.Engines, regionID uint64) error {
	err := engines.Raft.Update(func(txn *badger.Txn) error {
		return txn.Delete(meta.RaftStateKey(regionID))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	wb := new(engine_util.WriteBatch)
	wb.DeleteMeta(meta.PrepareBootstrapKey)
	// should clear raft initial state too.
	wb.DeleteMeta(meta.RegionStateKey(regionID))
	wb.DeleteMeta(meta.ApplyStateKey(regionID))
	err = engines.WriteKV(wb)
	if err != nil {
		return err
	}
	return nil
}

func ClearPrepareBootstrapState(engines *engine_util.Engines) error {
	err := engines.Kv.Update(func(txn *badger.Txn) error {
		return txn.Delete(meta.PrepareBootstrapKey)
	})
	return errors.WithStack(err)
}
