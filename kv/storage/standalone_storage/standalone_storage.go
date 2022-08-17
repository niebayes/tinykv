package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// reader for a snapshot of a standalone storage.
type StandAloneStorageReader struct {
	db   *badger.DB                  // the underlying store to read from.
	txn  *badger.Txn                 // the transaction wrapping these read operations on the snapshot.
	iter *engine_util.BadgerIterator // the iterator to the snapshot of the underlying store.
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if reader.iter == nil {
		reader.iter = engine_util.NewCFIterator(cf, reader.txn)
	}
	return reader.iter
}

func (reader *StandAloneStorageReader) Close() {
	if reader.iter != nil {
		reader.iter.Close()
		reader.iter = nil
	}
	if reader.txn != nil {
		reader.txn.Discard()
		reader.txn = nil
	}
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db     *badger.DB               // the underlying store.
	reader *StandAloneStorageReader // reader for a snapshot of the underlying store.
	cfg *config.Config 
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	storage := &StandAloneStorage{}
	storage.cfg = conf
	return storage
}

func (s *StandAloneStorage) Start() error {
	// open a badger DB as the underlying store.
	options := badger.DefaultOptions
	options.Dir = s.cfg.DBPath
	options.ValueDir = s.cfg.DBPath
	db, err := badger.Open(options)
	if err != nil {
		return err
	}
	s.db = db

	// new vs. &SomeType{} in Go.
	// @ref: https://stackoverflow.com/a/34543716
	s.reader = new(StandAloneStorageReader)
	s.reader.db = s.db  // associate the reader with the underlying store.
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db == nil {
		return errors.New("DB is not initialized correctly")
	}
	s.reader.Close()
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	// wrap the batch of writes to a WriteBatch.
	write_batch := new(engine_util.WriteBatch)
	for _, modify := range batch {
		write_batch.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}

	// perform the batch of writes.
	return write_batch.WriteToDB(s.db)
}

// FIXME: clarify the role of Reader.
// create a reader for the current snapshot of the underlying store.
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// close the last reader if exists.
	if s.reader != nil {
		s.reader.Close()
	}
	// start a new reader on the current snapshot, i.e. start a new transaction.
	s.reader.txn = s.db.NewTransaction(false) // read-only transaction.
	return s.reader, nil
}
