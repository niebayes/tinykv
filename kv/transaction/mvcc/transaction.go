package mvcc

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
		writes:  make([]storage.Modify, 0),
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, commitTs uint64, write *Write) {
	put := storage.Put{
		Key:   EncodeKey(key, commitTs),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	it := txn.Reader.IterCF(engine_util.CfLock)
	for it.Seek(key); it.Valid(); it.Next() {
		item := it.Item()
		if bytes.Equal(item.Key(), key) {
			val := make([]byte, 0)
			val, err := item.ValueCopy(val)
			if err != nil {
				return nil, err
			}
			lock, err := ParseLock(val)
			if err != nil {
				return nil, err
			}
			return lock, nil
		}
	}
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	put := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	del := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: del})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// to fing a value, we first examine the write column family since this column contains
	// commit timestamps. Inorder to provide snapshot isolation, only writes with commit timestamps
	// less than this txn's start timestamp are visible to this txn.

	// note, start timestamps and commit timestamps are both timestamps,
	// so we can use userkey + start timestamp to look up the write column.
	encodedKey := EncodeKey(key, txn.StartTS)

	// since the keys are sorted ascending by userkey, iter.Seek would stop seeking at
	// the smallest key with its userkey >= key.
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	for iter.Seek(encodedKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		// if this key is not the given key, all subsequent keys are also greater than the
		// given key. So we can stop the seeking early.
		if !bytes.Equal(userKey, key) {
			return nil, nil
		}

		// since write column contains write records, not values. We must further examine
		// the write kind of this record.
		// since the versions/records of a key are sorted descending by timestamps, the first version is
		// the latest version/record. So we examine it first.
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, err
		}

		switch write.Kind {
		case WriteKindPut:
			// this record corresponds to a put operation, so we can use the start timestamp to find
			// the written value of this put operation.
			encodedKey = EncodeKey(key, write.StartTS)
			iter = txn.Reader.IterCF(engine_util.CfDefault)
			iter.Seek(encodedKey)
			if !iter.Valid() {
				panic("a committed value must exist")
			}
			item = iter.Item()
			userKey = DecodeUserKey(item.Key())
			if !bytes.Equal(userKey, key) {
				panic("the key for a committed value must exist")
			}

			// retrieve the value.
			val = make([]byte, 0)
			val, err = item.ValueCopy(val)
			if err != nil {
				return nil, err
			}

			return val, nil

		case WriteKindDelete:
			// this record corresponds to a delete operation, this means the latest version of the key
			// is deleted. A delete operation is interpreted as deleting all versions of the key,
			// so we end the seeking immediately and reports that the given key does not exist.
			return nil, nil

		case WriteKindRollback:
			// this recod corresponds to a rollback operation, this means the latest version of the key
			// is rollbacked, i.e. it's invisible to all gets with start timestamps > the commit stamp
			// of this write record. So we proceed to the next version of the key.
			continue

		default:
			panic("invalid write kind")
		}
	}

	// the above seeking cannot find a visible value for the given key, report that the key does not exist.
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	put := storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	del := storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: del})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	// since verions/records of key are sorted descending by timestamp, we let the seeking
	// stops at the latest version of the version/record. This is because the write column
	// encodes key + commit timestamp where commit timestamp is greater than the start timestamp
	// of the corresponding write.
	// for e.g., if the start timestamp of the latest version of the key is X,
	// and this txn's start timestamp is also X. Then the seeking on the write column
	// won't stop at the corresponding write record of the latest version, since the commit timestamp
	// of the write record is greater than X and hence iter.Seek will skip it.
	// as a result, we cannot find the corresponding write with this txn's start timestamp.
	encodedKey := EncodeKey(key, math.MaxUint64)
	for iter.Seek(encodedKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, key) {
			return nil, 0, nil
		}

		val, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}

		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(item.Key()), nil
		}
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	encodedKey := EncodeKey(key, math.MaxUint64)
	for iter.Seek(encodedKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, key) {
			return nil, 0, nil
		}

		val, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}

		return write, decodeTimestamp(item.Key()), nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
