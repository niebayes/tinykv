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

// Transactional API.
// any error not needs to be known by the client is returned.
// any error needs to be known by the client is stored in the response.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// note, req.Context is used by router for routing the message to the expected tinykv server.
	// this is just a note, not a comment.

	key := req.GetKey()
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
	// Your Code Here (4B).
	return nil, nil
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
