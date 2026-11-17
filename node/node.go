package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	node *Node
}

type Node struct {
	leaderPort int32
	port       int32
	peers      []int32
}

func (n *Node) RegisterWithRegistry(registryAddress string) {
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

	n.peers = r.Ports
	log.Printf("Node on port %d registered with registry. Peers: %v", n.port, n.peers)
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

	node := &Node{port: int32(port)}

	grpcServer := grpc.NewServer()
	go grpcServer.Serve(lis)

	node.RegisterWithRegistry(registryAddress)

	select {}
}
