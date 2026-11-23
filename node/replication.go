package main

import (
	"context"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type replicationServer struct {
	pb.UnimplementedReplicateServiceServer
	node *Node
	// Election selects the highest surviving port. Under fail-stop with no
	// restarts, each replacement has a lower port, hence a newer ballot.
	// The promise and the accepted snapshot share the auction mutex.
	promisedWriter int32
	repairing      bool
}

func (s *replicationServer) eligible(writer int32) bool {
	n := s.node
	return writer > 0 && (writer == n.port || n.peerConns[writer] != nil) &&
		!n.IsElecting() && n.GetLeader() == writer
}

func newerWriter(candidate, previous int32) bool {
	return candidate != 0 && (previous == 0 || candidate < previous)
}

// prepareToServe runs with the auction mutex held. The surviving membership promises to
// fence older writers and reports what it accepted before making that promise.
func (s *replicationServer) prepareToServe(ctx context.Context) bool {
	a := &s.node.auction
	writer := s.node.port
	if !s.eligible(writer) || newerWriter(s.promisedWriter, writer) {
		return false
	}
	if a.snapshot.WriterPort == writer && s.promisedWriter == writer {
		return s.finishPending(ctx)
	}
	s.promisedWriter = writer
	best := a.snapshot
	ok := contactFollowers(ctx, s.node, func(ctx context.Context, client pb.ReplicateServiceClient) (*pb.AuctionState, bool) {
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
	a.needsCommit = true
	return s.finishPending(ctx)
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

// syncState runs with the auction mutex held. Once staging starts, a client
// disconnect does not interrupt the replication or commit phases.
func (s *replicationServer) syncState(ctx context.Context) bool {
	a := &s.node.auction
	ctx = context.WithoutCancel(ctx)
	snapshot := proto.CloneOf(a.snapshot)
	if !contactFollowers(ctx, s.node, func(ctx context.Context, client pb.ReplicateServiceClient) (bool, bool) {
		reply, err := client.Replicate(ctx, snapshot)
		return true, err == nil && reply.GetReply() == "success"
	}, func(bool) {}) || !s.eligible(snapshot.WriterPort) {
		return false
	}
	a.committed = snapshot
	return contactFollowers(ctx, s.node, func(ctx context.Context, client pb.ReplicateServiceClient) (bool, bool) {
		_, err := client.Commit(ctx, &pb.CommitRequest{Seq: snapshot.Seq, WriterPort: snapshot.WriterPort})
		return true, err == nil
	}, func(bool) {})
}

func (s *replicationServer) finishPending(ctx context.Context) bool {
	a := &s.node.auction
	if !a.needsCommit {
		return true
	}
	if !s.syncState(ctx) {
		s.scheduleRepair()
		return false
	}
	a.needsCommit = false
	return true
}

// Repair a partially staged write even if no further client request reaches
// the leader after election removes a crashed follower from the membership.
func (s *replicationServer) scheduleRepair() {
	if s.repairing {
		return
	}
	s.repairing = true
	go func() {
		a := &s.node.auction
		ticker := time.NewTicker(a.rpcTimeout)
		defer ticker.Stop()
		for range ticker.C {
			a.mu.Lock()
			if a.needsCommit && s.eligible(s.node.port) {
				if s.syncState(context.Background()) {
					a.needsCommit = false
				}
			}
			done := !a.needsCommit || s.node.GetLeader() != s.node.port
			if done {
				s.repairing = false
			}
			a.mu.Unlock()
			if done {
				return
			}
		}
	}()
}

func (s *replicationServer) Commit(_ context.Context, req *pb.CommitRequest) (*emptypb.Empty, error) {
	a := &s.node.auction
	a.mu.Lock()
	defer a.mu.Unlock()
	writer := req.GetWriterPort()
	if writer == s.node.port || !s.eligible(writer) || s.promisedWriter != writer ||
		a.snapshot.WriterPort != writer || a.snapshot.Seq != req.GetSeq() {
		return nil, status.Error(codes.FailedPrecondition, "commit does not match staged auction state")
	}
	a.committed = proto.CloneOf(a.snapshot)
	return &emptypb.Empty{}, nil
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

func contactFollowers[T any](ctx context.Context, n *Node, call func(context.Context, pb.ReplicateServiceClient) (T, bool), accept func(T)) bool {
	// Bound the wait for crash-stopped replicas using the healthy RPC round-trip bound.
	ctx, cancel := context.WithTimeout(ctx, n.auction.rpcTimeout)
	defer cancel()
	type vote struct {
		value T
		ok    bool
	}
	peers := n.GetPeers()
	votes := make(chan vote, len(peers))
	remaining := 0
	for _, peer := range peers {
		if peer == n.port {
			continue
		}
		conn := n.peerConns[peer]
		if conn == nil {
			return false
		}
		remaining++
		go func(client pb.ReplicateServiceClient) {
			value, ok := call(ctx, client)
			votes <- vote{value, ok}
		}(pb.NewReplicateServiceClient(conn))
	}
	for ; remaining > 0; remaining-- {
		select {
		case v := <-votes:
			if !v.ok {
				return false
			}
			accept(v.value)
		case <-ctx.Done():
			return false
		}
	}
	return true
}
