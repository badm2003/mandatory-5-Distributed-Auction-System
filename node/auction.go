package main

import (
	"context"
	"slices"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc/metadata"
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

func ack(reply string) *pb.Acknowledgement { return &pb.Acknowledgement{Reply: reply} }

func (s *auctionServer) Bid(ctx context.Context, req *pb.BidRequest) (*pb.Acknowledgement, error) {
	ids := metadata.ValueFromIncomingContext(ctx, bidderKey)
	if len(ids) != 1 || ids[0] == "" {
		return ack("EXCEPTION 1"), nil
	}

	id := ids[0]
	leader := s.election.GetLeader()
	if leader == 0 || s.election.IsElecting() {
		return ack("EXCEPTION 2"), nil
	}

	n := s.node
	a := &n.auctionState

	// In case this node is not the leader, forward the bid to the leader.
	if n.port != leader {
		conn := n.peerConns[leader]
		if conn == nil {
			return ack("EXCEPTION 3"), nil
		}

		ctx = metadata.AppendToOutgoingContext(ctx, bidderKey, id)
		reply, err := pb.NewAuctionClient(conn).Bid(ctx, req)

		if err != nil {
			return ack("EXCEPTION 4"), nil
		}

		return reply, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if s.election.IsElecting() || s.election.GetLeader() != leader {
		return ack("EXCEPTION 5"), nil
	}
	if ctx.Err() != nil {
		return ack("EXCEPTION 6"), nil
	}
	if a.syncedLeader != n.port {
		n.resyncLocked()
	}

	previousSeq := a.snapshot.Seq
	outcome := "TOO LOW"
	if !time.Now().Before(a.deadline) {
		outcome = "AUCTION CLOSED"
	} else {
		registered := slices.Contains(a.snapshot.Bidders, id)

		if !registered {
			a.snapshot.Bidders = append(a.snapshot.Bidders, id)
			a.snapshot.Seq++
		}

		// If the current snapshot does not have a bid or the new bid is higher than the current bid, update the snapshot.
		if !a.snapshot.HasBid || req.Amount > a.snapshot.Bid {
			a.snapshot.Bid = req.Amount
			a.snapshot.HighestBidder = id
			a.snapshot.HasBid = true
			a.snapshot.Seq++
			outcome = "OK"
		}
	}

	// We only need to commit if the snapshot has changed since the last commit.
	a.needsCommit = a.snapshot.Seq != previousSeq

	if err := s.replication.node.replicateAndCommit(); err != nil {
		return ack("EXCEPTION 7"), nil
	}
	return ack(outcome), nil
}

func (s *auctionServer) Result(ctx context.Context, req *emptypb.Empty) (*pb.ResultReply, error) {
	a := &s.node.auctionState
	for {
		a.mu.Lock()
		if !a.needsCommit {
			reply := &pb.ResultReply{
				Outcome: a.snapshot.Bid,
				Closed:  a.snapshot.Closed,
				Winner:  a.snapshot.HighestBidder,
				HasBid:  a.snapshot.HasBid,
			}
			a.mu.Unlock()
			return reply, nil
		}
		wait := a.commitSignal
		a.mu.Unlock()
		select {
		case <-wait: // Wait for the commit signal to be closed, indicating that a commit has occurred. (we try to respond again)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// This mutates the snapshot so it must be called with a lock
// Basically just changes the Closed bool to true if the auction has finished
// This is then replicated to all followers by the caller of this function
func (a *auctionState) closeIfDue() {
	if !a.snapshot.Closed && time.Now().After(a.deadline) {
		a.snapshot.Closed = true
		a.snapshot.Seq++
	}
}
