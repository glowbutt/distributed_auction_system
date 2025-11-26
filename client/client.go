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

	proto "auction-system/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const RequestTimeout = 5 * time.Second

type AuctionClient struct {
	id     string
	host   string
	port   int
	conn   *grpc.ClientConn
	client proto.AuctionClient
}

func NewAuctionClient(id, host string, port int) *AuctionClient {
	return &AuctionClient{id: id, host: host, port: port}
}

func (ac *AuctionClient) connect() error {
	if ac.conn != nil {
		_ = ac.conn.Close()
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ac.host, ac.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		ac.port++
		return err
	}
	ac.conn = conn
	ac.client = proto.NewAuctionClient(conn)
	return nil
}

func (ac *AuctionClient) Bid(amount int32) (*proto.BidResponse, error) {
	for {
		if ac.client == nil {
			_ = ac.connect()
		}
		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := ac.client.Bid(ctx, &proto.BidRequest{BidderId: ac.id, Amount: amount})
		cancel()
		if err != nil {
			ac.client = nil
			continue
		}
		return resp, nil
	}
}

func main() {
	id := flag.String("id", "", "Bidder ID")
	host := flag.String("host", "localhost", "Server host")
	port := flag.Int("startport", 8080, "Starting server port")
	flag.Parse()

	if *id == "" {
		fmt.Println("Bidder ID required")
		os.Exit(1)
	}

	client := NewAuctionClient(*id, *host, *port)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("=== Auction Client ===")
	fmt.Printf("Bidder ID: %s\n", *id)
	fmt.Println("Enter bids (e.g., '15' or 'bid 15'):")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		// Parse input
		parts := strings.Fields(text)
		var amtStr string
		if len(parts) == 1 {
			amtStr = parts[0]
		} else if len(parts) == 2 && strings.ToLower(parts[0]) == "bid" {
			amtStr = parts[1]
		} else {
			fmt.Println("Invalid input. Type a number or 'bid <amount>'.")
			continue
		}

		amt, err := strconv.Atoi(amtStr)
		if err != nil {
			fmt.Println("Invalid number")
			continue
		}

		resp, _ := client.Bid(int32(amt))
		fmt.Printf("\n%s\n\n", resp.Message)
	}
}
