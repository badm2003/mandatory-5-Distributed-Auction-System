package main

import (
	"context"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type replicationServer struct {
	pb.UnimplementedReplicateServiceServer
	node *Node
	// Election selects the highest surviving port. Under fail-stop with no
	// restarts, each replacement has a lower port, hence a newer ballot.
	// The promise and the accepted snapshot share the auction mutex.
	promisedWriter int32
}

func (s *replicationServer) eligible(writer int32) bool {
	n := s.node
	return writer > 0 && (writer == n.port || n.peerConns[writer] != nil) &&
		!n.IsElecting() && n.GetLeader() == writer
}

func newerWriter(candidate, previous int32) bool {
	return candidate != 0 && (previous == 0 || candidate < previous)
}

// prepareToServe runs with the auction mutex held. A majority promises to
// fence older writers and reports what it accepted before making that promise.
func (s *replicationServer) prepareToServe(ctx context.Context) bool {
	a := &s.node.auction
	writer := s.node.port
	if !s.eligible(writer) || newerWriter(s.promisedWriter, writer) {
		return false
	}
	if a.snapshot.WriterPort == writer && s.promisedWriter == writer {
		return true
	}
	s.promisedWriter = writer
	best := a.snapshot
	ok := collectQuorum(ctx, s.node, func(ctx context.Context, client pb.ReplicateServiceClient) (*pb.AuctionState, bool) {
		snapshot, err := client.Prepare(ctx, &pb.PrepareRequest{WriterPort: writer})
		return snapshot, err == nil
	}, func(snapshot *pb.AuctionState) {
		if newerWriter(snapshot.WriterPort, best.WriterPort) ||
			(snapshot.WriterPort == best.WriterPort && snapshot.Seq > best.Seq) {
			best = snapshot
		}
	})
	if !ok || !s.eligible(writer) {
		return false
	}
	a.snapshot = proto.CloneOf(best)
	a.snapshot.WriterPort = writer
	return true
}

func (s *replicationServer) Prepare(_ context.Context, req *pb.PrepareRequest) (*pb.AuctionState, error) {
	a := &s.node.auction
	a.mu.Lock()
	defer a.mu.Unlock()
	writer := req.GetWriterPort()
	if writer == s.node.port || !s.eligible(writer) || newerWriter(s.promisedWriter, writer) {
		return nil, status.Error(codes.FailedPrecondition, "auction writer unavailable or fenced")
	}
	s.promisedWriter = writer
	return proto.CloneOf(a.snapshot), nil
}

// syncState runs with the auction mutex held. A missed quorum leaves the
// operation's outcome unknown; retrying can propagate the latest snapshot.
func (s *replicationServer) syncState(ctx context.Context) bool {
	snapshot := proto.CloneOf(s.node.auction.snapshot)
	return collectQuorum(ctx, s.node, func(ctx context.Context, client pb.ReplicateServiceClient) (bool, bool) {
		reply, err := client.Replicate(ctx, snapshot)
		return true, err == nil && reply.GetReply() == "success"
	}, func(bool) {})
}

func (s *replicationServer) Replicate(_ context.Context, snapshot *pb.AuctionState) (*pb.Acknowledgement, error) {
	a := &s.node.auction
	a.mu.Lock()
	defer a.mu.Unlock()
	writer := snapshot.GetWriterPort()
	if writer == s.node.port || !s.eligible(writer) || newerWriter(s.promisedWriter, writer) {
		return nil, status.Error(codes.FailedPrecondition, "auction writer unavailable or fenced")
	}
	switch {
	case newerWriter(writer, a.snapshot.WriterPort), writer == a.snapshot.WriterPort && snapshot.Seq > a.snapshot.Seq:
		a.snapshot = proto.CloneOf(snapshot)
	case !proto.Equal(snapshot, a.snapshot):
		return ack("fail"), nil
	}
	s.promisedWriter = writer
	return ack("success"), nil
}

func collectQuorum[T any](ctx context.Context, n *Node, call func(context.Context, pb.ReplicateServiceClient) (T, bool), accept func(T)) bool {
	ctx, cancel := context.WithTimeout(ctx, n.auction.rpcTimeout)
	defer cancel()
	type vote struct {
		value T
		ok    bool
	}
	votes := make(chan vote, len(n.peerConns))
	for _, conn := range n.peerConns {
		go func(client pb.ReplicateServiceClient) {
			value, ok := call(ctx, client)
			votes <- vote{value, ok}
		}(pb.NewReplicateServiceClient(conn))
	}
	quorum := (len(n.peerConns)+1)/2 + 1
	accepted := 1 // Include the local promise or snapshot.
	for remaining := len(n.peerConns); accepted < quorum && accepted+remaining >= quorum; remaining-- {
		select {
		case v := <-votes:
			if v.ok {
				accept(v.value)
				accepted++
			}
		case <-ctx.Done():
			return false
		}
	}
	return accepted >= quorum
}
