package main

import (
	"context"
	"sync"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const bidderKey = "bidder-id"

// The election layer leads, this is isolated to auction.
type auctionLeadership interface {
	GetLeader() int32
	IsElecting() bool
}

type auctionServer struct {
	pb.UnimplementedAuctionServer
	node        *Node
	election    auctionLeadership
	replication *replicationServer
}

type auctionState struct {
	mu          sync.Mutex
	snapshot    *pb.AuctionState // latest staged state
	committed   *pb.AuctionState
	needsCommit bool
	deadline    time.Time
	rpcTimeout  time.Duration
}

func ack(reply string) *pb.Acknowledgement { return &pb.Acknowledgement{Reply: reply} }

func (s *auctionServer) Bid(ctx context.Context, req *pb.BidRequest) (*pb.Acknowledgement, error) {
	ids := metadata.ValueFromIncomingContext(ctx, bidderKey)
	if len(ids) != 1 || ids[0] == "" {
		return ack("exception"), nil
	}
	id := ids[0]
	leader := s.election.GetLeader()
	if leader == 0 || s.election.IsElecting() {
		return ack("exception"), nil
	}
	n := s.node
	a := &n.auction
	if n.port != leader {
		conn := n.peerConns[leader]
		if conn == nil {
			return ack("exception"), nil
		}
		ctx = metadata.AppendToOutgoingContext(ctx, bidderKey, id)
		reply, err := pb.NewAuctionClient(conn).Bid(ctx, req) // bid is a write so it gets sent to the leader

		if err != nil {
			return ack("exception"), nil
		}

		return reply, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if s.election.IsElecting() || s.election.GetLeader() != leader {
		return ack("exception"), nil
	}

	if ctx.Err() != nil {
		return ack("exception"), nil
	}

	if !s.replication.prepareToServe(ctx) {
		return ack("exception"), nil
	}

	previousSeq := a.snapshot.Seq
	outcome := "fail"
	if time.Now().Before(a.deadline) {
		registered := false
		for _, bidder := range a.snapshot.Bidders {
			if bidder == id {
				registered = true
				break
			}
		}

		if !registered {
			a.snapshot.Bidders = append(a.snapshot.Bidders, id)
			a.snapshot.Seq++
		}

		if !a.snapshot.HasBid || req.Amount > a.snapshot.Bid {
			a.snapshot.Bid = req.Amount
			a.snapshot.HighestBidder = id
			a.snapshot.HasBid = true
			a.snapshot.Seq++
			outcome = "success"
		}

	}
	a.needsCommit = a.snapshot.Seq != previousSeq
	if !s.replication.finishPending(ctx) {
		return ack("exception"), nil
	}
	return ack(outcome), nil
}

func (s *auctionServer) Result(ctx context.Context, req *emptypb.Empty) (*pb.ResultReply, error) {
	n := s.node
	a := &n.auction
	for {
		leader := s.election.GetLeader()
		if leader == 0 || s.election.IsElecting() {
			return nil, status.Error(codes.Unavailable, "auction leader unavailable")
		}
		a.mu.Lock()
		if s.election.GetLeader() != leader || s.election.IsElecting() {
			a.mu.Unlock()
			continue
		}
		if n.port == leader && !s.replication.prepareToServe(ctx) {
			a.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "auction recovery unavailable")
		}
		committed := a.committed
		if committed != nil && committed.WriterPort == leader &&
			a.snapshot.WriterPort == leader && committed.Seq == a.snapshot.Seq {
			result := &pb.ResultReply{Outcome: committed.Bid, HasBid: committed.HasBid, Closed: !time.Now().Before(a.deadline)}
			if result.Closed {
				result.Winner = committed.HighestBidder
			}
			a.mu.Unlock()
			return result, nil
		}
		pending := a.snapshot.WriterPort == leader

		a.mu.Unlock()
		if !pending {
			// The elected leader has not installed its recovered state here yet
			if conn := n.peerConns[leader]; conn != nil {
				return pb.NewAuctionClient(conn).Result(ctx, req)
			}
			return nil, status.Error(codes.Unavailable, "auction leader unavailable")
		}
		// Wait for Commit, checking for an election if the writer crashed mid-bid
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
