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

// The election layer owns leadership; auction code only reads it.
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
	mu         sync.Mutex
	snapshot   *pb.AuctionState
	deadline   time.Time
	rpcTimeout time.Duration
}

func ack(reply string) *pb.Acknowledgement { return &pb.Acknowledgement{Reply: reply} }

func (a *auctionState) closeIfDue() {
	if !a.snapshot.Closed && !time.Now().Before(a.deadline) {
		a.snapshot.Closed = true
		a.snapshot.Seq++
	}
}

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
		ctx, cancel := context.WithTimeout(ctx, 3*a.rpcTimeout)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, bidderKey, id)
		reply, err := pb.NewAuctionClient(conn).Bid(ctx, req)
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
	a.closeIfDue()
	outcome := "fail"
	if !a.snapshot.Closed {
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
	if !s.replication.syncState(ctx) {
		return ack("exception"), nil
	}
	return ack(outcome), nil
}

func (s *auctionServer) Result(ctx context.Context, req *emptypb.Empty) (*pb.ResultReply, error) {
	leader := s.election.GetLeader()
	if leader == 0 || s.election.IsElecting() {
		return nil, status.Error(codes.Unavailable, "auction leader unavailable")
	}
	n := s.node
	a := &n.auction
	if n.port != leader {
		conn := n.peerConns[leader]
		if conn == nil {
			return nil, status.Error(codes.Unavailable, "auction leader unavailable")
		}
		ctx, cancel := context.WithTimeout(ctx, 3*a.rpcTimeout)
		defer cancel()
		return pb.NewAuctionClient(conn).Result(ctx, req)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.election.IsElecting() || s.election.GetLeader() != leader {
		return nil, status.Error(codes.Unavailable, "auction leader unavailable")
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if !s.replication.prepareToServe(ctx) {
		return nil, status.Error(codes.Unavailable, "auction leadership quorum unavailable")
	}
	a.closeIfDue()
	if !s.replication.syncState(ctx) {
		return nil, status.Error(codes.Unavailable, "auction quorum unavailable")
	}
	result := &pb.ResultReply{Outcome: a.snapshot.Bid, HasBid: a.snapshot.HasBid, Closed: a.snapshot.Closed}
	if result.Closed {
		result.Winner = a.snapshot.HighestBidder
	}
	return result, nil
}
