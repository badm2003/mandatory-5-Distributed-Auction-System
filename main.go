package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedRegistryServer
	mu         sync.Mutex
	n          int
	registered []int32
	ready      chan struct{}
	duration   time.Duration
	deadline   time.Time
}

func (s *server) Register(ctx context.Context, req *pb.RegistrationRequest) (*pb.RegistrationReply, error) {
	log.Printf("[REGISTRY] Received registration request from port %d\n", req.Port)

	if req.Port < 1 || req.Port > 65535 {
		return nil, status.Error(codes.InvalidArgument, "invalid node port")
	}
	s.mu.Lock()
	if !slices.Contains(s.registered, req.Port) {
		if len(s.registered) == s.n {
			s.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "membership is full")
		}
		s.registered = append(s.registered, req.Port)
		if len(s.registered) == s.n {
			slices.Sort(s.registered)
			s.deadline = time.Now().Add(s.duration)
			close(s.ready)
		}
	}
	s.mu.Unlock()

	select {
	case <-s.ready:
	case <-ctx.Done():
		log.Printf("[REGISTRY] Context canceled for port %d: %v\n", req.Port, ctx.Err())
		return nil, ctx.Err()
	}

	return &pb.RegistrationReply{Ports: s.registered, DeadlineUnixNano: s.deadline.UnixNano()}, nil
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
	duration := flag.Duration("duration", 100*time.Second, "auction length after membership is formed")
	flag.Parse()
	if *n < 3 || *duration <= 0 {
		log.Fatal("need at least three nodes and a positive duration")
	}

	log.Printf("[REGISTRY] Starting on port %d, waiting for %d nodes...\n", *port, *n)

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("[REGISTRY] Failed to listen: %v", err)
	}

	server := &server{
		n:          *n,
		registered: make([]int32, 0, *n),
		ready:      make(chan struct{}),
		duration:   *duration,
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
