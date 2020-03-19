package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	//"golang.org/x/text/date"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage
	// used in 4A/4B
	Latches *latches.Latches //provides atomicity of TinyKV commands
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your code here (1).
	response := &kvrpcpb.RawGetResponse {
		NotFound: false,
	}
	key, cf, context := req.GetKey(), req.GetCf(), req.GetContext()
	reader, _ := server.storage.Reader(context)
	value ,err := reader.GetCF(cf, key)
	if err != nil {// err != nil represents failure
		response.NotFound = true
	} else {
		response.Value = value
	}
	response.Value = value
	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your code here (1).
	response := &kvrpcpb.RawPutResponse {}
	key, cf, context, value := req.GetKey(), req.GetCf(), req.GetContext(), req.GetValue()
	_ = server.storage.Write(context, []storage.Modify {
		{
			storage.Put{
				Key:   key,
				Value: value,
				Cf:    cf,
			},
		},
	})
	return response, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your code here (1).
	response := &kvrpcpb.RawDeleteResponse {}
	key, cf, context := req.GetKey(), req.GetCf(), req.GetContext()
	_ = server.storage.Write(context, []storage.Modify {
		{

			storage.Delete{
				Key:   key,
				Cf:    cf,
			},
		},
	})
	return response, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your code here (1).
	response := &kvrpcpb.RawScanResponse {}
	starkey, cf, context, limit:= req.GetStartKey(), req.GetCf(), req.GetContext(), req.GetLimit()
	reader, _ := server.storage.Reader(context)
	iter := reader.IterCF(cf)// reverse: false
	// Seek will storage the seek result into it's item
	for iter.Seek(starkey) ; iter.Valid() && uint32(len(response.Kvs)) < limit; iter.Next(){
		item := iter.Item()
		var key, value []byte
		key = item.KeyCopy(nil)
		value, _ = item.ValueCopy(nil)
		fmt.Println(key,value)
		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	return response, nil
}

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
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your code here (4B).
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your code here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your code here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your code here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your code here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your code here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your code here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	return &coprocessor.Response{}, nil
}
