package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Storage represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type Storage interface {
	Start() error // init if necessary.
	Stop() error  // clean up if necessary.

	// @note: ctx context is unused until project 4.
	Write(ctx *kvrpcpb.Context, batch []Modify) error   // write a batch of modifications to the underlying store.
	Reader(ctx *kvrpcpb.Context) (StorageReader, error) // get the reader of the underlying store.
}

// reader of the underlying store. All read operations go through the reader.
type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error) // retrieve the value for the key cf_key in the cf column family.
	IterCF(cf string) engine_util.DBIterator     // return the iterator for the cf column family.
	Close()                                      // close the reader of the underlying store.
}
