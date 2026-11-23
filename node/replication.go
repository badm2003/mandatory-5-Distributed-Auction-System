package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type replicationServer struct {
	pb.UnimplementedReplicateServiceServer
	node *Node
}

func (s *replicationServer) Replicate(ctx context.Context, snapshot *pb.AuctionState) (*pb.Acknowledgement, error) {
	a := &s.node.auctionState
	a.mu.Lock()
	defer a.mu.Unlock()
	a.snapshot = proto.CloneOf(snapshot)
	a.needsCommit = true
	return ack("OK"), nil
}

func (s *replicationServer) Commit(ctx context.Context, req *pb.CommitRequest) (*emptypb.Empty, error) {
	a := &s.node.auctionState
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.snapshot.Seq == req.Seq && a.needsCommit {
		a.applyStaged()
	}
	return &emptypb.Empty{}, nil
}

func (n *Node) resyncLocked() {
	a := &n.auctionState
	// If this node has been elected leader, it may have a pending write that was not committed before the election.
	// If so, it must finish the write before accepting new bids.
	if a.needsCommit {
		a.applyStaged()
	}

	a.closeIfDue()
	n.replicateAndCommit()
	a.syncedLeader = n.port
}

func (n *Node) replicateAndCommit() error {
	st := proto.CloneOf(n.auctionState.snapshot)
	err_rep := n.toAllFollowers(func(ctx context.Context, c pb.ReplicateServiceClient) error {
		_, err := c.Replicate(ctx, st)
		return err
	})
	if err_rep != nil {
		log.Printf("Node on port %d failed to replicate to all followers: %v", n.port, err_rep)
		return err_rep
	}

	// All followers have acknowledged the replication, so we can now commit the state locally.
	n.auctionState.applyStaged()

	// Now we can commit the state on all followers.
	err_com := n.toAllFollowers(func(ctx context.Context, c pb.ReplicateServiceClient) error {
		_, err := c.Commit(ctx, &pb.CommitRequest{Seq: st.Seq})
		return err
	})
	if err_com != nil {
		log.Printf("Node on port %d failed to commit to all followers: %v", n.port, err_com)
		return err_com
	}

	return nil
}

func (n *Node) toAllFollowers(call func(context.Context, pb.ReplicateServiceClient) error) error {
	var wg sync.WaitGroup
	for _, peer := range n.GetPeers() {
		conn := n.peerConns[peer]
		if peer == n.port || conn == nil {
			continue
		}

		wg.Add(1)
		go func(peer int32, client pb.ReplicateServiceClient) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), n.auctionState.rpcTimeout)
			defer cancel()
			err := call(ctx, client)
			if err != nil {
				log.Printf("Node on port %d failed to replicate to peer %d: %v", n.port, peer, err)
			}
		}(peer, pb.NewReplicateServiceClient(conn))
	}

	wg.Wait()
	return nil
}

func (a *auctionState) applyStaged() {
	a.committed = proto.CloneOf(a.snapshot)
	a.needsCommit = false
	close(a.commitSignal)
	a.commitSignal = make(chan struct{})
}

// This loop runs every second and checks if it has been elected leader.
// If it has and it hasnt realized yet, it will resync with the other nodes to ensure it has the latest state.
func (n *Node) leaderLoop() {
	for {
		time.Sleep(time.Second)

		// If we are not the leader or we are in the process of electing we don't need to do anything
		if n.GetLeader() != n.port || n.IsElecting() {
			continue
		}

		a := &n.auctionState
		a.mu.Lock()
		if a.syncedLeader != n.port {
			// If we are the leader yet we haven't synced with the other nodes yet, we need to do that now
			n.resyncLocked()
		} else if !a.snapshot.Closed && !time.Now().Before(a.deadline) {
			// If the auction has finished, we need to close it and replicate that to the other nodes
			a.closeIfDue()
			n.replicateAndCommit()
		}
		a.mu.Unlock()
	}
}
