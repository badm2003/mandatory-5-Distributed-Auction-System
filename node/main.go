package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	port      int32 // this and below never changes so a lock is not needed
	conns     map[int32]pb.ElectionClient
	peerConns map[int32]*grpc.ClientConn

	electionState electionState
	auctionState  auctionState
}

type electionState struct {
	mu                sync.Mutex
	leaderPort        int32
	peers             []int32
	electing          bool
	electionStartTime time.Time
}

type auctionState struct {
	mu           sync.Mutex
	syncedLeader int32            // the last leader we synced with
	snapshot     *pb.AuctionState // latest staged state
	committed    *pb.AuctionState // latest committed state
	needsCommit  bool
	commitSignal chan struct{}
	deadline     time.Time
	rpcTimeout   time.Duration
}

func main() {
	registryPort := flag.Int("registry-port", 5000, "port the registry listens on")
	portFlag := flag.Int("port", 0, "local node port (0 picks a free port)")
	rpcTimeout := flag.Duration("rpc-timeout", time.Second, "peer RPC timeout; set above the network round-trip bound")
	flag.Parse()
	if *registryPort < 1 || *registryPort > 65535 || *rpcTimeout <= 0 {
		log.Fatal("invalid registry port or RPC timeout")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *portFlag))
	if err != nil {
		log.Fatalf("Node failed to listen: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port

	log.Printf("Node listening on port %d", port)
	registryAddress := fmt.Sprintf("localhost:%d", *registryPort)

	node := &Node{
		port:  int32(port),
		conns: make(map[int32]pb.ElectionClient),
	}

	grpcServer := grpc.NewServer()
	pb.RegisterElectionServer(grpcServer, &electionServer{node: node})

	membership := node.RegisterWithRegistry(registryAddress)
	node.registerAuctionServices(grpcServer, membership, *rpcTimeout)

	// We create our connections to our peers first and then we serve the gRPC server
	node.ConnectToPeers()
	go grpcServer.Serve(lis)

	node.StartElection()
	go node.Heartbeat()

	select {}
}

func (n *Node) registerAuctionServices(server *grpc.Server, membership *pb.RegistrationReply, timeout time.Duration) {
	n.auctionState = auctionState{
		deadline:     time.Unix(0, membership.DeadlineUnixNano),
		rpcTimeout:   timeout,
		snapshot:     &pb.AuctionState{},
		committed:    &pb.AuctionState{},
		needsCommit:  false,
		commitSignal: make(chan struct{}),
	}

	// Keep connections for every registered peer; election supplies the live membership.
	n.peerConns = make(map[int32]*grpc.ClientConn, len(membership.Ports)-1)
	for _, peer := range membership.Ports {
		if peer == n.port {
			continue
		}
		conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", peer), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to auction peer %d: %v", peer, err)
		}
		n.peerConns[peer] = conn
	}

	replica := &replicationServer{node: n}
	pb.RegisterReplicateServiceServer(server, replica)
	pb.RegisterAuctionServer(server, &auctionServer{node: n, election: n, replication: replica})

	go n.leaderLoop()
}
