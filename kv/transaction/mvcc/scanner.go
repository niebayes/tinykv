package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	startTS  uint64
	reader   storage.StorageReader
	iter     engine_util.DBIterator
	init     bool
	startKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	scan := &Scanner{
		startTS:  txn.StartTS,
		reader:   txn.Reader,
		init:     false,
		startKey: startKey,
	}
	return scan
}

func (scan *Scanner) Close() {
	if scan.init {
		scan.iter.Close()
	}
	scan.reader.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	if !scan.init {
		scan.iter = scan.reader.IterCF(engine_util.CfWrite)
		encodedKey := EncodeKey(scan.startKey, TsMax)
		scan.iter.Seek(encodedKey)
		scan.init = true
	}

	for scan.iter.Valid() {
		item := scan.iter.Item()
		key := make([]byte, 0)
		key = item.KeyCopy(key)
		commitTS := decodeTimestamp(key)
		if commitTS > scan.startTS {
			scan.iter.Next()
			continue
		}

		userKey := DecodeUserKey(key)

		// val := make([]byte, 0)
		// val, err := item.ValueCopy(val)
		// if err != nil {
		// 	panic(err)
		// }
		// write, err := ParseWrite(val)
		// if err != nil {
		// 	panic(err)
		// }

		txn1 := NewMvccTxn(scan.reader, commitTS)
		value, err := txn1.GetValue(userKey)
		if err != nil {
			panic(err)
			// or continue?
		}

		// we only collect the latest version visible to this txn for each unique user key.
		scan.iter.Next()
		for scan.iter.Valid() {
			item = scan.iter.Item()
			userKey1 := DecodeUserKey(item.Key())
			if !bytes.Equal(userKey, userKey1) {
				break
			}
			scan.iter.Next()
		}

		// if the value is rollbacked or deleted, then GetValue will return a nil value and hence
		// we shall skip this value.
		if value == nil {
			continue
		}

		return userKey, value, nil
	}

	return nil, nil, nil
}
