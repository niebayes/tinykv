package engine_util

import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/gogo/protobuf/proto"
)

// construct a key of a column family cf_key from the given cf column family and the key key.
func KeyWithCF(cf string, key []byte) []byte {
	// append a slice to the end of another slice.
	return append([]byte(cf+"_"), key...)
}

// perform a Get operation on a column family in the db.
func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	// anonymous function and closure in Go.
	// @ref: https://programming.guide/go/anonymous-function-literal-lambda-closure.html

	// this statement first defines an anonymous function inside the parameter scope and
	// then pass it as an argument to the db.View function. The db.View function then executes
	// the function and relays its return value, i.e. of type error, to here.

	// the txn is created inside the db.View function and we here define what the txn shall do.
	err = db.View(func(txn *badger.Txn) error {
		// note, val, err are assigned and hence "=", but not initialized and therefore ":=".
		val, err = GetCFFromTxn(txn, cf, key)
		return err
	})

	// named return in Go.
	// @ref: https://go.dev/ref/spec#Return_statements

	// after compilation, the val and err are ordinary local variables inside the function and initialized
	// as the default value of their type. The following statements in the function just assign value to these
	// variables.
	return
}

// perform a Get operation on a column family through transaction.
func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) {
	item, err := txn.Get(KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	// txn.Get returns a reference to the value, and here we make a deep copy of the value
	// and return the copy instead.
	val, err = item.ValueCopy(val)
	return
}

// perform a Put operation on a column family through transaction.
func PutCF(db *badger.DB, cf string, key []byte, val []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(KeyWithCF(cf, key), val)
	})
}

// perform a Delete operation on a column family through transaction.
func DeleteCF(db *badger.DB, cf string, key []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Delete(KeyWithCF(cf, key))
	})
}

// perform Delete operations on a range of keys. Values of these keys in all column families are deleted.
func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	// wrap the Delete operations to a batch of write.
	// @note: Go adopts garbage collecion, so no need to explicitly free a newed object.
	batch := new(WriteBatch)
	// FIXME: why Delete is wrapped into a read-only txn?
	txn := db.NewTransaction(false) // for read-only transaction, set update to false.
	defer txn.Discard()

	// each key has been associated a set of column families.
	// to delete a key, values in all column families must be deleted all together.
	for _, cf := range CFs {
		// append these writes/deletes to the batch.
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	// perform the batch of writes/deletes.
	return batch.WriteToDB(db)
}

// append the deletes of keys in range [startKey, endKey) to the batch.
func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil) // passing in nil to indicate creating a new dst slice.
		// FIXME: why call ExceedEndKey after the check it.Valid() returns true?
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

// returns true if the current key exceeds over or equal the endKey key.
func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}

func GetMeta(db *badger.DB, key []byte, msg proto.Message) error {
	var val []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func GetMetaFromTxn(txn *badger.Txn, key []byte, msg proto.Message) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func PutMeta(db *badger.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}
