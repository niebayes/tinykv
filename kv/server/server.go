package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

// FIXME: what does the underline "_" mean?
var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// any error not needs to be known by the client is returned.
// any error needs to be known by the client is stored in the response.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// note, req.Context is used by router for routing the message to the expected tinykv server.
	// this is just a note, not a comment.

	key := req.GetKey()
	// in the Percolator paper, to access a row in the database, you need to use the row txn
	// provided by Bigtable. In tinykv, there's no such a row txn feature. So we use a latch
	// per key, aka. a latch per row, to provide atomicity of accessing a row.
	keysToLatch := [][]byte{key}
	// wait for latches and then acquire the latches.
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	response := &kvrpcpb.GetResponse{NotFound: true}

	// MvccTxn is the interface to the percolator worker in the server side.
	// so accesses on server's storage must go through a MvccTxn object.
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	// check if there's a pending lock on this key.
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	// even if a lock with start timestamp > txn.StartTS is found, it does not matter.
	// since a write txn starts after this txn shall not affect this txn, because its
	// write is invisible to this txn.
	// techniquely speaking, due to percolator uses snapshot isolation level, read uncommitted
	// data, aka. dirty reads, is forbidden. So if a write starts before this txn but not
	// committed yet, this get must abort. Or we have a dirty read anamoly.
	if lock != nil && lock.IsLockedFor(key, txn.StartTS, response) {
		return response, nil
	}

	// get the latest version of the key visible to this txn.
	val, err := txn.GetValue(key)
	if err != nil {
		return nil, err
	}
	response.Value = val

	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// note, separating prewrites for primary and secondary rows are controlled by the client, not this server.

	response := &kvrpcpb.PrewriteResponse{}
	response.Errors = make([]*kvrpcpb.KeyError, 0)

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	// lock all keys to be examined.
	keysToLatch := make([][]byte, 0)
	muts := req.GetMutations()
	for _, mut := range muts {
		keysToLatch = append(keysToLatch, mut.GetKey())
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	// check if there's a write-write conflict for each key.
	conflict := false
	for i := 0; i < len(muts); i++ {
		key := muts[i].GetKey()
		_, commitTS, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if commitTS >= txn.StartTS {
			// found a write-write conflict. A txn committed after the start of this txn. So this write
			// is not visible to this txn. If this txn contains writes on the same key, then the update
			// of the committed write belong to the other txn is lost, hence a lost update which shall
			// be avoided in snapshot isolation.

			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: commitTS,
					Key:        key,
					Primary:    req.GetPrimaryLock(),
				},
			})
			conflict = true
		}
	}
	if conflict {
		return response, nil
	}

	// check if there's a pending lock for each key.
	locked := false
	for i := 0; i < len(muts); i++ {
		key := muts[i].GetKey()
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			// note, no need to check for the lock's start timestamp. Since we are about to
			// write the row, we have to take the only lock.
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{Locked: lock.Info(key)})
			locked = true
		}
	}
	if locked {
		return response, nil
	}

	// group all writes: writes on data column and writes on lock column.
	for _, mut := range muts {
		// write lock.
		lock := &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      txn.StartTS,
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(mut.GetOp()),
		}
		txn.PutLock(mut.GetKey(), lock)

		// write data.
		if mut.GetOp() == kvrpcpb.Op_Put {
			txn.PutValue(mut.GetKey(), mut.GetValue())
		} else if mut.GetOp() == kvrpcpb.Op_Del {
			txn.DeleteValue(mut.GetKey())
		} else {
			panic("invalid op type")
		}
	}

	// write all writes to disk.
	if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}

	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// note, separating commits for primary and secondary rows are controlled by the client, not this server.

	response := &kvrpcpb.CommitResponse{}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	keys := req.GetKeys()
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	// check if this is a duplicate commit request.
	for _, key := range keys {
		// CurrentWrite will return a write whose start timestamp == txn.StartTS.
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// if the returned write is not nil, then this commit request is duplicated since they
		// convey the same start timestamp and hence they correspond to the same txn.
		if write != nil {
			if write.Kind == mvcc.WriteKindDelete || write.Kind == mvcc.WriteKindRollback {
				response.Error = &kvrpcpb.KeyError{Abort: "true"}
			}
			return response, nil
		}
	}

	// collect writes' kind for each key from locks.
	writeKinds := make([]mvcc.WriteKind, 0)

	// ensure the locks are present currently.
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		// if the lock does not exist or the lock is alternated by a different lock from another txn, abort.
		if lock == nil || lock.Ts != txn.StartTS {
			response.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return response, nil
		}
		writeKinds = append(writeKinds, lock.Kind)
	}

	// group deletions of all locks and puts of all writes.
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		txn.DeleteLock(key)
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{StartTS: txn.StartTS, Kind: writeKinds[i]})
	}

	// write to disk.
	if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, nil
}

// CheckTxnStatus reports on the status of a transaction and may take action to
// rollback expired locks.
// If the transaction has previously been rolled back or committed, return that information.
// If the TTL of the transaction is exhausted, abort that transaction and roll back the primary lock.
// Otherwise, returns the TTL information.
// type CheckTxnStatusRequest struct {
// 	Context              *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
// 	PrimaryKey           []byte   `protobuf:"bytes,2,opt,name=primary_key,json=primaryKey,proto3" json:"primary_key,omitempty"`
// 	LockTs               uint64   `protobuf:"varint,3,opt,name=lock_ts,json=lockTs,proto3" json:"lock_ts,omitempty"`
// 	CurrentTs            uint64   `protobuf:"varint,4,opt,name=current_ts,json=currentTs,proto3" json:"current_ts,omitempty"`

// type CheckTxnStatusResponse struct {
// 	RegionError *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
// 	// Three kinds of txn status:
// 	// locked: lock_ttl > 0
// 	// committed: commit_version > 0
// 	// rolled back: lock_ttl == 0 && commit_version == 0
// 	LockTtl       uint64 `protobuf:"varint,2,opt,name=lock_ttl,json=lockTtl,proto3" json:"lock_ttl,omitempty"`
// 	CommitVersion uint64 `protobuf:"varint,3,opt,name=commit_version,json=commitVersion,proto3" json:"commit_version,omitempty"`
// 	// The action performed by TinyKV in response to the CheckTxnStatus request.
// 	Action               Action   `protobuf:"varint,4,opt,name=action,proto3,enum=kvrpcpb.Action" json:"action,omitempty"`

// const (
// 	Action_NoAction Action = 0
// The lock is rolled back because it has expired.
// 	Action_TTLExpireRollback Action = 1
// The lock does not exist, TinyKV left a record of the rollback, but did not
// have to delete a lock.
// 	Action_LockNotExistRollback Action = 2
// )

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	response := &kvrpcpb.CheckTxnStatusResponse{}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// note, this txn's timestamp is lock.Ts not the timestamp of a new txn.
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	key := req.GetPrimaryKey()
	// CurrentWrite will retrieve the write with commitTS == txn.StartTS.
	// as a side note, when we rollback a txn/lock, we simultaneously write a write record
	// with commitTS == txn.StartTS == lock.Ts.
	write, commitTS, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	if write != nil {
		// has write.

		if write.Kind == mvcc.WriteKindRollback {
			// this txn is already rollbacked.
			// FIXME: why set commitTS to 0
			response.CommitVersion = uint64(0)
			return response, nil
		}
		// otherwise, this txn is already committed.
		response.CommitVersion = commitTS
		return response, nil
	}
	// no write.

	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		// no lock, no write, just put a rollback write to tell the subsequent txns that it's rollbacked,
		// although there's nothing to rollback. Since the subsequent txn's does not need to know whether
		// there's something rollbacked or not.
		response.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(key, req.GetLockTs(), &mvcc.Write{StartTS: req.GetLockTs(), Kind: mvcc.WriteKindRollback})

		if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
			return nil, err
		}
		return response, nil
	}
	// has write and lock.

	// the lock is alternated by another txn.
	if lock.Ts != txn.StartTS {
		return response, nil
	}

	// check lock's ttl to check if this txn is timed out.
	currentTime := mvcc.PhysicalTime(req.GetCurrentTs())
	expireTime := mvcc.PhysicalTime(lock.Ts) + lock.Ttl
	if currentTime >= expireTime {
		// remove the expired lock and the value if any.
		txn.DeleteLock(key)
		if lock.Kind == mvcc.WriteKindPut {
			txn.DeleteValue(key)
		}
		// write a record to indicate this write is rollbacked.
		// the reason why we set the commitTS of this rollback write record is that the CurrentWrite
		// will find this write record and so kvCheckTxnStatus will inspect this record and find this
		// lock is already rollbacked
		txn.PutWrite(key, req.GetLockTs(), &mvcc.Write{StartTS: req.GetLockTs(), Kind: mvcc.WriteKindRollback})

		if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
			return nil, err
		}

		response.Action = kvrpcpb.Action_TTLExpireRollback
		return response, nil
	}

	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	response := &kvrpcpb.BatchRollbackResponse{}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	keys := req.GetKeys()

	for _, key := range keys {
		// check if this request is duplicated.
		write, commitTS, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// if the write is already rollbacked.
			if write.Kind == mvcc.WriteKindRollback && commitTS == txn.StartTS {
				return response, nil
			}
			// if the write is already committed.
			if write.Kind == mvcc.WriteKindPut && commitTS > txn.StartTS {
				response.Error = &kvrpcpb.KeyError{Abort: "true"}
				return response, nil
			}
		}

		// check if the lock is still present.
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		// if the lock is missing, this write may be already rollbacked or committed.
		// but we still write a rollback record as the test suite requires.
		if lock != nil && lock.Ts == txn.StartTS {
			// delete the lock and the corresponding value.
			txn.DeleteLock(key)
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
		}
		// leave a rollback indicator to the write column.
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback})
	}

	if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}

	return response, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
