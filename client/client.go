package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/zMyLlama/mandatory-5-Distributed-Auction-System/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	ports := flag.String("nodes", "", "comma-separated node ports, e.g. 50001,50002,50003")
	flag.Parse()

	list := strings.Split(*ports, ",")
	current := 0
	connect := func() pb.AuctionClient {
		conn, _ := grpc.NewClient("127.0.0.1:"+list[current], grpc.WithTransportCredentials(insecure.NewCredentials()))
		return pb.NewAuctionClient(conn)
	}
	client := connect()

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
				current = (current + 1) % len(list)
				client = connect()
				fmt.Println("Node unreachable, switched to", list[current], ". Try again")
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
				current = (current + 1) % len(list)
				client = connect()
				fmt.Println("Node unreachable, switched to", list[current], ". Try again")
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
