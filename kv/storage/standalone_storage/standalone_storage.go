package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/write_batch"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// reader for a standalone storage.
type StandAloneStorageReader struct {
	db *badger.DB // the underlying store to read from.
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(reader.db, cf, key)
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := reader.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (reader *StandAloneStorageReader) Close() {
	
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db     *badger.DB // the underlying store.
	reader *StandAloneStorageReader
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	storage := &StandAloneStorage{}
	storage.db = new(badger.DB)
	storage.reader.db = storage.db 
	return storage
}

func (s *StandAloneStorage) Start() error {
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

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.reader == nil {
		return nil, errors.New("reader is not created yet")
	}
	return s.reader, nil
}
