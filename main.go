package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedRegistryServer
	mu         sync.Mutex
	n          int
	registered []int32
	ready      chan struct{}
}

func (s *server) Register(ctx context.Context, req *pb.RegistrationRequest) (*pb.RegistrationReply, error) {
	log.Printf("[REGISTRY] Received registration request from port %d\n", req.Port)

	s.mu.Lock()
	s.registered = append(s.registered, req.Port)
	if len(s.registered) == cap(s.registered) {
		close(s.ready)
	}
	s.mu.Unlock()

	select {
	case <-s.ready:
	case <-ctx.Done():
		log.Printf("[REGISTRY] Context canceled for port %d: %v\n", req.Port, ctx.Err())
		return nil, ctx.Err()
	}

	return &pb.RegistrationReply{Ports: s.registered}, nil
}

func spawnNodes(n, registryPort int) []*exec.Cmd {
	var cmds []*exec.Cmd
	for i := range n {
		cmd := exec.Command("go", "run", "./node", "--registry-port", strconv.Itoa(registryPort))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			log.Fatalf("[REGISTRY] Failed to start node %d: %v", i, err)
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

func main() {
	port := flag.Int("port", 5000, "port the registry listens on")
	n := flag.Int("n", 3, "number of nodes to wait for")
	spawn := flag.Bool("spawn", true, "whether to spawn nodes or not")
	flag.Parse()

	log.Printf("[REGISTRY] Starting on port %d, waiting for %d nodes...\n", *port, *n)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("[REGISTRY] Failed to listen: %v", err)
	}

	server := &server{
		n:          *n,
		registered: make([]int32, 0, *n),
		ready:      make(chan struct{}),
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRegistryServer(grpcServer, server)
	go grpcServer.Serve(lis)

	log.Printf("[REGISTRY] Waiting for %d nodes to register...\n", *n)

	cmds := []*exec.Cmd{}
	if *spawn {
		cmds = spawnNodes(*n, *port)
	}

	<-server.ready

	log.Printf("[REGISTRY] All %d nodes have registered. Notifying them...\n", *n)

	log.Printf("[REGISTRY] All nodes have been notified. Shutting down...\n")
	grpcServer.GracefulStop()

	for _, cmd := range cmds {
		cmd.Wait()
	}

	time.Sleep(1 * time.Second)
}
