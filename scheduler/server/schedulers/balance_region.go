// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// TODO(3C): figure out how the peers, stores, regions, raft groups transit.

type Store struct {
	id         uint64
	regionSize int64
	// this is used to select the target store to which the peer moved in.
	// the target store is the store with small enough region score and the highest
	// distinct score.
	// small enough = sourceStore.regionSize - targetStore.regionSize > 2*region.ApproximateSize.
	// region score = region size.
	// distinct score = sourceStore.regionCount[regionId] - targetStore.regionCount[regionId]
	// where regionId is the id of the region belong to which the peer is moved out and in.
	distinctScore int64
}

// copied from randRegion in region.go
func (s *balanceRegionScheduler) randRegion(regions core.RegionsContainer, opts ...core.RegionOption) *core.RegionInfo {
	for i := 0; i < balanceRegionRetryLimit; i++ {
		region := regions.RandomRegion(nil, nil)
		if region == nil {
			return nil
		}
		isSelect := true
		for _, opt := range opts {
			if !opt(region) {
				isSelect = false
				break
			}
		}
		if isSelect {
			return region
		}
	}
	return nil
}

func checkScheduleLeastReplicas(maxReplicas int, region *metapb.Region) bool {
	return len(region.GetPeers()) >= maxReplicas
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// retrieve store ids and region sizes.
	stores := make([]Store, 0)
	for _, storeInfo := range cluster.GetStores() {
		if storeInfo.IsUp() && storeInfo.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, Store{id: storeInfo.GetID(), regionSize: storeInfo.GetRegionSize()})
		}
	}

	// sort stores by region size. Stores with larger region size are placed first.
	sort.SliceStable(stores, func(i, j int) bool {
		return stores[i].regionSize >= stores[j].regionSize
	})

	// try to select a store from which peers moved out.
	// since the balance scheduler try to balance load on every store, it will first
	// try to select a pending region. A region having pending peers, i.e. peers being
	// installing a snapshot or waiting to install a snapshot, is regarded as pending region.
	// Since pending peers may indicate the store is overloaded, i.e. its disk is overloaded,
	// that's why we shall first examine pending regions.
	// if there's no pending regions, we shall next examine stores having follower regions.
	// A follower region is such a region belong to which the peers running on the store are
	// all follower peers.
	// if there's no follower regions, we shall finally examine stores having leader regions.
	// A leader region is such a region belong to which the peer running on the store is a leader
	// peer.
	// The reason why we first examine follower regions is that moving follower peers does not involve
	// leader transfer.
	var outRegion *core.RegionInfo = nil
	var outStore Store
	for _, store := range stores {
		var regionsContainer core.RegionsContainer

		// this creates a function closure in which regionsContainer references regionsContainer outside the function.
		// so the assignment inside the function closure is visible outside the function.
		cluster.GetPendingRegionsWithLock(store.id, func(rc core.RegionsContainer) { regionsContainer = rc })
		// select a random region regardless its start key and end key, also allow the selected region contains
		// pending peers. Pending peers may indicate the store containing region is overloaded currently.
		// note, randRegion will retry RandomRegion for 10 times.
		if regionInfo := s.randRegion(regionsContainer, core.HealthRegionAllowPending()); regionInfo != nil {
			// note, maxReplicas is the max number of peers of a region.
			// it's the peer to be scheduled, hence if the region the peer belongs to does not have
			// enough #replicas, i.e. #peers, we shall wait until #peers of this region reaches or
			// grows over maxReplicas.
			// this means peer moving in and out only happens on regions with enough peers.
			// if a region has no enough peers, we just let the region to split until it has
			// enough #peers, aka. number of maxReplicas peers.
			if checkScheduleLeastReplicas(cluster.GetMaxReplicas(), regionInfo.GetMeta()) {
				outRegion = regionInfo
				outStore = store
				break
			}
		}

		cluster.GetFollowersWithLock(store.id, func(rc core.RegionsContainer) { regionsContainer = rc })
		// does not need to inspect pending regions now since we've inspected it before.
		if regionInfo := s.randRegion(regionsContainer, core.HealthRegion()); regionInfo != nil {
			if checkScheduleLeastReplicas(cluster.GetMaxReplicas(), regionInfo.GetMeta()) {
				outRegion = regionInfo
				outStore = store
				break
			}
		}

		cluster.GetLeadersWithLock(store.id, func(rc core.RegionsContainer) { regionsContainer = rc })
		if regionInfo := s.randRegion(regionsContainer, core.HealthRegion()); regionInfo != nil {
			if checkScheduleLeastReplicas(cluster.GetMaxReplicas(), regionInfo.GetMeta()) {
				outRegion = regionInfo
				outStore = store
				break
			}
		}
	}

	// no suitable region to be moved out.
	if outRegion == nil {
		return nil
	}

	// try to select a store to which the region moved in.
	// the target store is the store with small enough region score and with the
	// highest distinct score.

	// collect all stores with small enough region score.
	candidateTargetStores := make([]Store, 0)
	for i := len(stores) - 1; i >= 0; i-- {
		store := stores[i]

		// since stores are sorted by region size, if this scan encounters the source store,
		// any remaining stores won't be selected since the region size diff must be negative.
		if outStore.id == store.id {
			break
		}

		// again, since stores are sorted by region size, if this store is not small enough,
		// any remaining stores won't be selected.
		// note, store's regionSize is the total size of all regions in the store.
		// if the region size diff between the two stores is not greater than the size
		// of the moved region, the next time Schedule is called, this region may be
		// moved back.
		if outStore.regionSize-store.regionSize <= 2*int64(outRegion.GetApproximateSize()) {
			break
		}

		sourceScore := 0
		targetScore := 0
		for _, peer := range outRegion.GetPeers() {
			if peer.StoreId == outStore.id {
				sourceScore++
			}
			if peer.StoreId == store.id {
				targetScore++
			}
		}
		store.distinctScore = int64(sourceScore) - int64(targetScore)

		candidateTargetStores = append(candidateTargetStores, store)
	}

	// no valid stores to be selected as candidate target stores.
	if len(candidateTargetStores) == 0 {
		return nil
	}

	// sort candidate target stores by distinct score first, and then by region score.
	sort.SliceStable(candidateTargetStores, func(i, j int) bool {
		if candidateTargetStores[i].distinctScore == candidateTargetStores[j].distinctScore {
			return candidateTargetStores[i].regionSize <= candidateTargetStores[j].regionSize
		}
		return candidateTargetStores[i].distinctScore > candidateTargetStores[j].distinctScore
	})

	// the target store is the one with highest distinct score.
	inStore := candidateTargetStores[0]

	// create a new peer in the inStore.
	peer, err := cluster.AllocPeer(inStore.id)
	if err != nil {
		return nil
	}

	desc := fmt.Sprintf("move region %v from store %v to store %v", outRegion.GetID(), outStore.id, inStore.id)
	operator, err := operator.CreateMovePeerOperator(desc, cluster, outRegion, operator.OpBalance, outStore.id, inStore.id, peer.GetId())
	if err != nil {
		return nil
	}
	return operator
}
