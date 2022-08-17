package server

import (
	"context"
	"errors"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// part of methods of the TinyKvServer interface are implemented in server.go
// the remaining methods are implemented in this file.
// together, these methods of the kv.server.Server implements the TinyKvServer interface.

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}

	// get the reader of the underlying store.
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	// perform Get operation through the reader.
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}

	// compose the response.
	res := &kvrpcpb.RawGetResponse{
		Value: val,
	}

	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}

	// wrap the put request into a batch of modifications.
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	mod := storage.Modify{
		Data: put,
	}
	batch := []storage.Modify{mod}

	// perform the batch of modifications on the underlying store.
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}

	// compose the response.
	res := &kvrpcpb.RawPutResponse{} // currently nothing to fill in.

	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}

	// wrap the delete request into a batch of modifications.
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	mod := storage.Modify{
		Data: del,
	}
	batch := []storage.Modify{mod}

	// perform the batch of modifications on the underlying store.
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}

	// compose the response.
	res := &kvrpcpb.RawDeleteResponse{} // currently nothing to fill in.

	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	// collect the values from the start key (may be absent).
	kv_pairs := make([]*kvrpcpb.KvPair, 0)

	var cnt uint32 = 0
	iter := reader.IterCF(req.GetCf())
	for iter.Seek(req.GetStartKey()); iter.Valid() && cnt < req.GetLimit(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			break
		}
		kv_pairs = append(kv_pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
	}

	// compose the response.
	res := &kvrpcpb.RawScanResponse{
		Kvs: kv_pairs,
	}

	return res, nil
}
