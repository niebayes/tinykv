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

// type PrewriteRequest struct {
// 	Context   *Context    `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
// 	Mutations []*Mutation `protobuf:"bytes,2,rep,name=mutations" json:"mutations,omitempty"`
// 	// Key of the primary lock.
// 	PrimaryLock          []byte   `protobuf:"bytes,3,opt,name=primary_lock,json=primaryLock,proto3" json:"primary_lock,omitempty"`
// 	StartVersion         uint64   `protobuf:"varint,4,opt,name=start_version,json=startVersion,proto3" json:"start_version,omitempty"`
// 	LockTtl              uint64   `protobuf:"varint,5,opt,name=lock_ttl,json=lockTtl,proto3" json:"lock_ttl,omitempty"`

// type Mutation struct {
// 	Op                   Op       `protobuf:"varint,1,opt,name=op,proto3,enum=kvrpcpb.Op" json:"op,omitempty"`
// 	Key                  []byte   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
// 	Value                []byte   `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`

// type KeyError struct {
// 	Locked               *LockInfo      `protobuf:"bytes,1,opt,name=locked" json:"locked,omitempty"`
// 	Retryable            string         `protobuf:"bytes,2,opt,name=retryable,proto3" json:"retryable,omitempty"`
// 	Abort                string         `protobuf:"bytes,3,opt,name=abort,proto3" json:"abort,omitempty"`
// 	Conflict             *WriteConflict `protobuf:"bytes,4,opt,name=conflict" json:"conflict,omitempty"`

// type Error struct {
// Message              string          `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
// NotLeader            *NotLeader      `protobuf:"bytes,2,opt,name=not_leader,json=notLeader" json:"not_leader,omitempty"`
// RegionNotFound       *RegionNotFound `protobuf:"bytes,3,opt,name=region_not_found,json=regionNotFound" json:"region_not_found,omitempty"`
// KeyNotInRegion       *KeyNotInRegion `protobuf:"bytes,4,opt,name=key_not_in_region,json=keyNotInRegion" json:"key_not_in_region,omitempty"`
// EpochNotMatch        *EpochNotMatch  `protobuf:"bytes,5,opt,name=epoch_not_match,json=epochNotMatch" json:"epoch_not_match,omitempty"`
// StaleCommand         *StaleCommand   `protobuf:"bytes,7,opt,name=stale_command,json=staleCommand" json:"stale_command,omitempty"`
// StoreNotMatch        *StoreNotMatch  `protobuf:"bytes,8,opt,name=store_not_match,json=storeNotMatch" json:"store_not_match,omitempty"`

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	response := &kvrpcpb.PrewriteResponse{}
	// response.Errors = make([]*kvrpcpb.KeyError, len(req.GetMutations()))
	response.Errors = make([]*kvrpcpb.KeyError, 0)

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
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
	abort := false
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

			// FIXME: What string to fill in Abort?
			// response.Errors[i] = &kvrpcpb.KeyError{Abort: "true"}
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: commitTS,
					Key:        key,
					Primary:    req.GetPrimaryLock(),
				},
			})
			abort = true
		}
	}
	if abort {
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
			// response.Errors[i] = &kvrpcpb.KeyError{Locked: lock.Info(key)}
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{Locked: lock.Info(key)})
			locked = true
		}
	}
	if locked {
		return response, nil
	}

	// group all writes on lock and write column families.
	for _, mut := range muts {
		lock := &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      txn.StartTS,
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(mut.GetOp()),
		}
		txn.PutLock(mut.GetKey(), lock)
		txn.PutValue(mut.GetKey(), mut.GetValue())
	}

	// write all writes to disk.
	if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}

	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
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
