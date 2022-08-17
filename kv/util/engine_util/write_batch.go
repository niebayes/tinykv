// defines a WriteBatch to specify a batch of writes, i.e. Puts and Deletes.
// provides helper functions for performing a batch of writes.

package engine_util

import (
	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
)

type WriteBatch struct {
	entries []*badger.Entry // the entries are placed in a linear array.

	// fields below are used for stuff corresponding to safe point.
	size          int // number of bytes of the linear array.
	safePoint     int
	safePointSize int
	safePointUndo int
}

const (
	CfDefault string = "default"
	CfWrite   string = "write"
	CfLock    string = "lock"
)

var CFs [3]string = [3]string{CfDefault, CfWrite, CfLock}

// get #writes in the batch.
func (wb *WriteBatch) Len() int {
	return len(wb.entries)
}

// append a Put operation to the batch of writes.
// the Put operation is not performed until WriteToDB is called.
func (wb *WriteBatch) SetCF(cf string, key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   KeyWithCF(cf, key),
		Value: val,
	})
	wb.size += len(key) + len(val)
}

// append a Delete operation to the batch of writes.
// the Delete operation is not performed until WriteToDB is called.
func (wb *WriteBatch) DeleteCF(cf string, key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: KeyWithCF(cf, key),
	})
	wb.size += len(key)
}

func (wb *WriteBatch) SetMeta(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
	return nil
}

func (wb *WriteBatch) DeleteMeta(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += len(key)
}

func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.size = wb.safePointSize
}

// perform a batch write to the db. If fails on any write, return the error.
func (wb *WriteBatch) WriteToDB(db *badger.DB) error {
	if len(wb.entries) > 0 {
		err := db.Update(func(txn *badger.Txn) error {
			for _, entry := range wb.entries {
				var err1 error
				// if no value is provided, it's a Delete operation. Otherwise, it's a Put operation.
				if len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// perform a batch write to the db. If fails on any write, panic.
func (wb *WriteBatch) MustWriteToDB(db *badger.DB) {
	err := wb.WriteToDB(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) Reset() {
	wb.entries = wb.entries[:0] // slice all entries out.
	wb.size = 0
	wb.safePoint = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}
