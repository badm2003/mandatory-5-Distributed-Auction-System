package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	nodePort := flag.Int("node-port", 0, "port of the node to connect to")
	flag.Parse()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", *nodePort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Client failed to connect: %v", err)
	}

	log.Printf("Client connected to node on port %d", *nodePort)

	client := pb.NewAuctionClient(conn)

	name := ""

	for {
		fmt.Println("Type a command (bid <amount>, result or exit):")
		if name == "" {
			fmt.Println("Set your name first via: name <text>")
		}
		br := bufio.NewScanner(os.Stdin)
		br.Scan()
		cmd := br.Text()

		switch {
		case cmd == "exit":
			return
		case cmd == "result":
			if name == "" {
				fmt.Println("Not allowed. Mising name!")
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			reply, err := client.Result(ctx, &emptypb.Empty{})
			cancel()
			if err != nil {
				fmt.Printf("Result not available, try again: %v", err)
				continue
			}
			fmt.Printf("Result: %v\n", reply)
		case len(cmd) > 4 && cmd[:4] == "bid ":
			if name == "" {
				fmt.Println("Not allowed. Mising name!")
				continue
			}
			amount, err := strconv.Atoi(cmd[4:])
			if err != nil {
				fmt.Println("Invalid amount")
				continue
			}

			ctx := metadata.AppendToOutgoingContext(context.Background(), "bidder-id", name)
			reply, err := client.Bid(ctx, &pb.BidRequest{Amount: int64(amount)})
			if err != nil {
				fmt.Println("Error occurred while placing bid")
				continue
			}
			fmt.Printf("Bid result: %v\n", reply)
		case len(cmd) > 5 && cmd[:5] == "name ":
			name = cmd[5:]
		default:
			fmt.Println("Unknown command")
		}

		fmt.Println()
	}
}
