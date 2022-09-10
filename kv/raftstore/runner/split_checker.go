package runner

import (
	"encoding/hex"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type SplitCheckTask struct {
	Region *metapb.Region
}

type splitCheckHandler struct {
	engine  *badger.DB
	router  message.RaftRouter
	checker *sizeSplitChecker
}

func NewSplitCheckHandler(engine *badger.DB, router message.RaftRouter, conf *config.Config) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine:  engine,
		router:  router,
		checker: newSizeSplitChecker(conf.RegionMaxSize, conf.RegionSplitSize),
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) Handle(t worker.Task) {
	spCheckTask, ok := t.(*SplitCheckTask)
	if !ok {
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	region := spCheckTask.Region
	regionId := region.Id
	log.Debugf("executing split check worker.Task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(region.StartKey), hex.EncodeToString(region.EndKey))
	key := r.splitCheck(regionId, region.StartKey, region.EndKey)
	// key != nil iff region size > split size.
	if key != nil {
		_, userKey, err := codec.DecodeBytes(key)
		if err == nil {
			// It's not a raw key.
			// To make sure the keys of same user key locate in one Region, decode and then encode to truncate the timestamp
			key = codec.EncodeBytes(userKey)
		}
		msg := message.Msg{
			Type:     message.MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &message.MsgSplitRegion{
				RegionEpoch: region.GetRegionEpoch(),
				SplitKey:    key,
			},
		}
		err = r.router.Send(regionId, msg)
		if err != nil {
			log.Warnf("failed to send check result: [regionId: %d, err: %v]", regionId, err)
		}
	} else {
		log.Debugf("no need to send, split key not found: [regionId: %v]", regionId)
	}
}

/// SplitCheck gets the split keys by scanning the range.
func (r *splitCheckHandler) splitCheck(regionID uint64, startKey, endKey []byte) []byte {
	txn := r.engine.NewTransaction(false)
	defer txn.Discard()

	// reset curSize and splitKey.
	r.checker.reset()
	it := engine_util.NewCFIterator(engine_util.CfDefault, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		// scan keys in range [start key, end key)
		if engine_util.ExceedEndKey(key, endKey) {
			// update region size
			r.router.Send(regionID, message.Msg{
				Type: message.MsgTypeRegionApproximateSize,
				Data: r.checker.currentSize,
			})
			break
		}
		// accumulate len(key) + len(value) to curSize.
		// if curSize > splitSize, record set split key to the current key, but do not terminate scan.
		// since we need to update region size.
		// if curSize > maxSize, terminate scan immediately.
		// maxSize delimits that one round of scan won't last too long in that although the key range
		// is small but the number of items in the range large.
		if r.checker.onKv(key, item) {
			break
		}
	}
	return r.checker.getSplitKey()
}

type sizeSplitChecker struct {
	maxSize   uint64
	splitSize uint64

	currentSize uint64
	splitKey    []byte
}

func newSizeSplitChecker(maxSize, splitSize uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:   maxSize,
		splitSize: splitSize,
	}
}

func (checker *sizeSplitChecker) reset() {
	checker.currentSize = 0
	checker.splitKey = nil
}

func (checker *sizeSplitChecker) onKv(key []byte, item engine_util.DBItem) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	if checker.currentSize > checker.splitSize && checker.splitKey == nil {
		checker.splitKey = util.SafeCopy(key)
	}
	return checker.currentSize > checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKey() []byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize < checker.maxSize {
		checker.splitKey = nil
	}
	return checker.splitKey
}
