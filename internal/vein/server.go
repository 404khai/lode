package vein

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lodev1 "github.com/404khai/lode/gen/lode/v1"
	"github.com/404khai/lode/internal/nugget"
)

// StreamChunk is the size of data messages on shard streams.
const StreamChunk = 1 << 20

// maxShard bounds a single PutShard so a client cannot exhaust memory.
const maxShard = nugget.MaxSize + 1<<20

// Server exposes a Store over gRPC.
type Server struct {
	lodev1.UnimplementedVeinServiceServer
	store *Store
	id    string
}

func NewServer(store *Store, id string) *Server { return &Server{store: store, id: id} }

func toStatus(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrCorrupt):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func parseID(b []byte) (nugget.ID, error) {
	id, err := nugget.FromBytes(b)
	if err != nil {
		return id, status.Error(codes.InvalidArgument, err.Error())
	}
	return id, nil
}

func (s *Server) PutShard(stream lodev1.VeinService_PutShardServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	id, err := parseID(first.GetId())
	if err != nil {
		return err
	}
	n := first.GetLength()
	if n < 0 || n > maxShard {
		return status.Errorf(codes.InvalidArgument, "shard length %d out of range", n)
	}
	if ok, err := s.store.Has(id); err != nil {
		return toStatus(err)
	} else if ok {
		// Drain the stream; the bytes are already here.
		for {
			if _, err := stream.Recv(); errors.Is(err, io.EOF) {
				return stream.SendAndClose(&lodev1.PutShardResponse{Deduplicated: true})
			} else if err != nil {
				return err
			}
		}
	}
	buf := make([]byte, 0, n)
	buf = append(buf, first.GetData()...)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if int64(len(buf)+len(msg.GetData())) > n {
			return status.Error(codes.InvalidArgument, "shard longer than declared length")
		}
		buf = append(buf, msg.GetData()...)
	}
	if int64(len(buf)) != n {
		return status.Errorf(codes.InvalidArgument, "shard has %d bytes, declared %d", len(buf), n)
	}
	if nugget.Sum(buf) != id {
		return status.Error(codes.InvalidArgument, "shard bytes do not match id")
	}
	if err := s.store.Put(id, buf); err != nil {
		return toStatus(err)
	}
	return stream.SendAndClose(&lodev1.PutShardResponse{})
}

func (s *Server) GetShard(req *lodev1.GetShardRequest, stream lodev1.VeinService_GetShardServer) error {
	id, err := parseID(req.GetId())
	if err != nil {
		return err
	}
	data, err := s.store.Get(id)
	if err != nil {
		return toStatus(err)
	}
	for off := 0; off < len(data) || off == 0; off += StreamChunk {
		end := min(off+StreamChunk, len(data))
		if err := stream.Send(&lodev1.GetShardResponse{Data: data[off:end]}); err != nil {
			return err
		}
		if end == len(data) {
			break
		}
	}
	return nil
}

func (s *Server) StatShard(_ context.Context, req *lodev1.StatShardRequest) (*lodev1.StatShardResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	ok, err := s.store.Has(id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &lodev1.StatShardResponse{Exists: ok}, nil
}

func (s *Server) DeleteShard(_ context.Context, req *lodev1.DeleteShardRequest) (*lodev1.DeleteShardResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.store.Delete(id); err != nil {
		return nil, toStatus(err)
	}
	return &lodev1.DeleteShardResponse{}, nil
}

func (s *Server) Status(context.Context, *lodev1.StatusRequest) (*lodev1.StatusResponse, error) {
	capacity, used, err := s.store.Usage()
	if err != nil {
		return nil, toStatus(err)
	}
	return &lodev1.StatusResponse{VeinId: s.id, CapacityBytes: capacity, UsedBytes: used}, nil
}

// Usage reports the filesystem capacity of the data directory and the bytes
// held in segments.
func (s *Store) Usage() (capacity, used int64, err error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &fs); err != nil {
		return 0, 0, err
	}
	capacity = int64(fs.Blocks) * int64(fs.Bsize) //nolint:unconvert,gosec // field types differ by OS; sizes fit in int64
	nums, err := s.segments()
	if err != nil {
		return 0, 0, err
	}
	for _, n := range nums {
		if fi, err := os.Stat(s.segPath(n)); err == nil {
			used += fi.Size()
		}
	}
	return capacity, used, nil
}
