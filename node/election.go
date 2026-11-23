package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type electionServer struct {
	pb.UnimplementedElectionServer
	node *Node
}

func (s *electionServer) Heartbeat(ctx context.Context, in *emptypb.Empty) (*pb.Acknowledgement, error) {
	log.Printf("Node on port %d received heartbeat from peer", s.node.port)
	return &pb.Acknowledgement{Reply: "OK"}, nil
}

func (s *electionServer) Election(ctx context.Context, in *pb.ElectionRequest) (*pb.Acknowledgement, error) {
	log.Printf("Node on port %d received election request from peer %d", s.node.port, in.Ports[int32(len(in.Ports)-1)])

	s.node.electionState.mu.Lock()

	// First we check if the election round-trip is completed, which is the case if the current node's port is already in the list of ports.
	if slices.Contains(in.Ports, s.node.port) {
		log.Printf("Node on port %d is already in the list of ports. Round-trip assumed completed.", s.node.port)

		s.node.electionState.mu.Unlock()
		s.BroadcastElected(in)

		return &pb.Acknowledgement{Reply: "OK"}, nil
	}

	// If we are already electing and the starting port of the election request is less than our port, we can ignore the request,
	// since it means that the election has already been started by a node with a lower port number.
	if s.node.electionState.electing && in.StartingPort < s.node.port {
		log.Printf("Node on port %d ignoring election request from peer %d", s.node.port, in.StartingPort)
		s.node.electionState.mu.Unlock()
		// gRPC just expects some response, so we can just return an acknowledgement with a reply of "IGNORED" or well anything else.
		return &pb.Acknowledgement{Reply: "IGNORED"}, nil
	}

	if !s.node.electionState.electing {
		s.node.electionState.electing = true
		s.node.electionState.electionStartTime = time.Now()
	}

	s.node.electionState.mu.Unlock()

	// I was thinking about purging in-flight of sorts but it is probably better to just purge the peers when the round-trip is completed, since we can be sure they are dead at that point.
	// I will leave this code here for now (commented out), as a sort of proof of concept.
	/* In case of this we can purge the peers between the two last ports in the list, since they did not respond and are likely dead.
	if len(in.Ports) >= 2 {
		previousPeer := in.Ports[len(in.Ports)-1]
		peerBeforePrevious := in.Ports[len(in.Ports)-2]

		log.Printf("Node on port %d purging peers between %d and %d", s.node.port, peerBeforePrevious, previousPeer)

		newPeers := []int32{}
		purging := false
		for i, peer := range s.node.peers {
			if peer == peerBeforePrevious {
				purging = true
				newPeers = append(newPeers, s.node.peers[i])
				continue
			} else if peer == previousPeer {
				purging = false
				newPeers = append(newPeers, s.node.peers[i])
				continue
			} else if purging {
				log.Printf("Node on port %d purging peer %d", s.node.port, peer)
				delete(s.node.conns, peer)
			} else {
				newPeers = append(newPeers, s.node.peers[i])
			}
		}
		s.node.peers = newPeers
	} */

	s.node.ForwardElectionMessage(in)

	return &pb.Acknowledgement{Reply: "OK"}, nil
}

func (s *electionServer) BroadcastElected(in *pb.ElectionRequest) {
	log.Printf("Node on port %d broadcasting elected message to peers", s.node.port)

	s.node.electionState.mu.Lock()

	// Determine the leader port, which is the highest port in the list of ports.
	highestPort := int32(0)
	for _, port := range in.Ports {
		if port > highestPort {
			highestPort = port
		}
	}
	log.Printf("Node on port %d determined leader port to be %d", s.node.port, highestPort)

	s.node.electionState.peers = in.Ports
	s.node.electionState.leaderPort = highestPort
	s.node.electionState.electing = false
	peers := s.node.electionState.peers
	leaderPort := s.node.electionState.leaderPort

	s.node.electionState.mu.Unlock()

	for _, peer := range peers {
		if peer == s.node.port {
			continue
		}

		// Broadcast is just a send and pray.
		log.Printf("Node on port %d broadcasting elected message to peer %d", s.node.port, peer)
		_, err := s.node.conns[peer].Elected(context.Background(), &pb.ElectedRequest{
			LeaderPort: leaderPort,
			Ports:      peers,
		})
		if err != nil {
			log.Printf("Node on port %d failed to broadcast elected message to peer %d: %v", s.node.port, peer, err)
		}
	}
}

func (s *electionServer) Elected(ctx context.Context, in *pb.ElectedRequest) (*pb.Acknowledgement, error) {
	log.Printf("Node on port %d received elected message from peer %d", s.node.port, in.LeaderPort)
	s.node.electionState.mu.Lock()
	defer s.node.electionState.mu.Unlock()

	log.Printf("Node on port %d declaring peer %d as leader", s.node.port, in.LeaderPort)
	s.node.electionState.leaderPort = in.LeaderPort
	s.node.electionState.electing = false
	s.node.electionState.peers = in.Ports

	log.Printf("Node on port %d updated leader to %d and peers to %v", s.node.port, s.node.electionState.leaderPort, s.node.electionState.peers)

	return &pb.Acknowledgement{Reply: "OK"}, nil
}

func (n *Node) ForwardElectionMessage(electionRequest *pb.ElectionRequest) {
	n.electionState.mu.Lock()
	nextLogicalPeer := n.GetNextLogicalPeer()
	client := n.conns[nextLogicalPeer]
	n.electionState.mu.Unlock()
	if nextLogicalPeer == -1 {
		log.Printf("Node on port %d failed to get next logical peer", n.port)
		return
	}

	if nextLogicalPeer == n.port {
		log.Printf("Node on port %d is the only node left, declaring self as leader", n.port)

		n.electionState.mu.Lock()
		n.electionState.peers = []int32{n.port}

		n.electionState.leaderPort = n.port
		n.electionState.electing = false
		n.electionState.mu.Unlock()

		return
	}

	// We need to create a new election request with the current node's port added to the list of ports.
	// In case of failure we avoid mutating the original election request, so we can try to forward it again to the next logical peer.
	electionRequestPorts := electionRequest.Ports
	newElectionRequest := &pb.ElectionRequest{
		Ports:        append(electionRequestPorts, n.port),
		StartingPort: electionRequest.StartingPort,
	}
	log.Printf("Node on port %d forwarding election request to peer %d", n.port, nextLogicalPeer)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := client.Election(ctx, newElectionRequest)
	cancel()
	if err != nil {
		log.Printf("Node on port %d failed to forward election request to peer %d: %v", n.port, nextLogicalPeer, err)
		// If forwarding fails, we can assume the next logical peer is down and declare it as dead, then try to forward the message to the next logical peer.
		log.Printf("Node on port %d declaring peer %d as dead", n.port, nextLogicalPeer)
		n.electionState.mu.Lock()
		newPeers := []int32{}
		for _, peer := range n.electionState.peers {
			if peer != nextLogicalPeer {
				newPeers = append(newPeers, peer)
			}
		}
		n.electionState.peers = newPeers
		n.electionState.mu.Unlock()

		n.ForwardElectionMessage(electionRequest) // Try to forward the message to the next logical peer
	}
}

func (n *Node) GetNextLogicalPeer() int32 {
	// Find the next logical peer in the list of peers
	// This is really just finding yourself in the list of peers and returning the next one, or the first one if you're the last.
	for i, peer := range n.electionState.peers {
		if peer == n.port {
			if i == len(n.electionState.peers)-1 {
				return n.electionState.peers[0]
			}
			return n.electionState.peers[i+1]
		}
	}
	return -1 // Should never happen
}

func (n *Node) Heartbeat() {
	for {
		time.Sleep(5 * time.Second) // Heartbeat interval

		n.electionState.mu.Lock()
		electing := n.electionState.electing
		timedOut := electing && (time.Since(n.electionState.electionStartTime) > 10*time.Second)
		peers := n.electionState.peers
		n.electionState.mu.Unlock()

		if timedOut {
			log.Printf("Node on port %d election timeout. Restarting election...", n.port)
			go n.StartElection()

			continue // Skip sending heartbeats while electing
		}
		if electing {
			log.Printf("Node on port %d is currently electing. Skipping heartbeat.", n.port)
			continue // Skip sending heartbeats while electing
		}

		for _, peer := range peers {
			if peer == n.port {
				continue // Don't send heartbeat to self
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := n.conns[peer].Heartbeat(ctx, &emptypb.Empty{})
			cancel()
			if err != nil {
				// If a heartbeat fails we start an election
				log.Printf("Node on port %d failed to send heartbeat to peer %d: %v. Starting election...", n.port, peer, err)
				go n.StartElection()

				break // Break out of the loop to avoid sending heartbeats to other peers while an election is in progress
			}
		}

	}
}

func (n *Node) StartElection() {
	log.Printf("Node on port %d starting election...", n.port)

	n.electionState.mu.Lock()
	nextLogicalPeer := n.GetNextLogicalPeer()
	n.electionState.electing = true
	n.electionState.electionStartTime = time.Now()
	n.electionState.mu.Unlock()
	if nextLogicalPeer == -1 { // should never happen
		log.Printf("Node on port %d failed to get next logical peer for election", n.port)
		return
	}

	n.ForwardElectionMessage(&pb.ElectionRequest{
		Ports:        []int32{},
		StartingPort: n.port,
	})
}

func (n *Node) RegisterWithRegistry(registryAddress string) *pb.RegistrationReply {
	log.Printf("Node on port %d attempting to register with registry at %s", n.port, registryAddress)

	conn, err := grpc.NewClient(registryAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to registry: %v", err)
	}
	defer conn.Close()

	log.Printf("Node on port %d connected to registry at %s", n.port, registryAddress)
	log.Printf("Node on port %d sending registration request to registry...", n.port)
	client := pb.NewRegistryClient(conn)
	r, err := client.Register(context.Background(), &pb.RegistrationRequest{
		Port: n.port,
	})
	if err != nil {
		log.Fatalf("Failed to register with registry: %v", err)
	}

	n.electionState.peers = r.Ports
	log.Printf("Node on port %d registered with registry. Peers: %v", n.port, n.electionState.peers)
	return r
}

func (n *Node) ConnectToPeers() {
	for _, peer := range n.electionState.peers {
		if peer == n.port {
			continue
		}

		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", peer), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to peer %d: %v", peer, err)
		}
		n.conns[peer] = pb.NewElectionClient(conn)
	}
}

// All functions below are just getters for the election state.
// It is expected that our other services access anything election related through these functions.

func (n *Node) GetLeader() int32 {
	n.electionState.mu.Lock()
	defer n.electionState.mu.Unlock()
	return n.electionState.leaderPort
}

func (n *Node) GetPeers() []int32 {
	n.electionState.mu.Lock()
	defer n.electionState.mu.Unlock()
	return n.electionState.peers
}

func (n *Node) IsElecting() bool {
	n.electionState.mu.Lock()
	defer n.electionState.mu.Unlock()
	return n.electionState.electing
}

func (n *Node) IsLeader() bool {
	n.electionState.mu.Lock()
	defer n.electionState.mu.Unlock()
	return n.electionState.leaderPort == n.port
}
