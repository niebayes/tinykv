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
			outRegion = regionInfo
			outStore = store
			break
		}

		cluster.GetFollowersWithLock(store.id, func(rc core.RegionsContainer) { regionsContainer = rc })
		// does not need to inspect pending regions now since we've inspected it before.
		if regionInfo := s.randRegion(regionsContainer, core.HealthRegion()); regionInfo != nil {
			outRegion = regionInfo
			outStore = store
			break
		}

		cluster.GetLeadersWithLock(store.id, func(rc core.RegionsContainer) { regionsContainer = rc })
		if regionInfo := s.randRegion(regionsContainer, core.HealthRegion()); regionInfo != nil {
			outRegion = regionInfo
			outStore = store
			break
		}
	}

	// no suitable region to be moved out.
	if outRegion == nil {
		return nil
	}

	// try to select a store to which the region moved in.
	// the selection starts from regions with smaller region sizes.
	var inStore Store
	for i := len(stores) - 1; i >= 0; i-- {
		store := stores[i]

		// the out store and the in store cannot be identical.
		if outStore.id == store.id {
			continue
		}

		// note, store's regionSize is the total size of all regions in the store.
		// if the region size diff between the two stores is not greater than the size
		// of the moved region, the next time Schedule is called, this region may be
		// moved back.
		if outStore.regionSize-store.regionSize > 2*int64(outRegion.GetApproximateSize()) {
			inStore = store
			break
		}
	}

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
