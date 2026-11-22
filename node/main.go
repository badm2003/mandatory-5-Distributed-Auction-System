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
)

type Node struct {
	port  int32                       // never changes so a lock is not needed for this
	conns map[int32]pb.ElectionClient // never changes so a lock is not needed for this

	electionState electionState
}

type electionState struct {
	mu                sync.Mutex
	leaderPort        int32
	peers             []int32
	electing          bool
	electionStartTime time.Time
}

func main() {
	registryPort := flag.Int("registry-port", 5000, "port the registry listens on")
	flag.Parse()

	lis, err := net.Listen("tcp", ":0")
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

	node.RegisterWithRegistry(registryAddress)

	// We create our connections to our peers first and then we serve the gRPC server
	node.ConnectToPeers()
	go grpcServer.Serve(lis)

	node.StartElection()
	go node.Heartbeat()

	select {}
}
