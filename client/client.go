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
		ac.conn = nil
		ac.client = nil
	}
	addr := fmt.Sprintf("%s:%d", ac.host, ac.port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// try next port on failure
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
		if ac.client == nil {
			// small backoff to avoid tight loop while rotating ports
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := ac.client.Bid(ctx, &proto.BidRequest{BidderId: ac.id, Amount: amount})
		cancel()
		if err != nil {
			// close and try next server
			if ac.conn != nil {
				_ = ac.conn.Close()
			}
			ac.client = nil
			ac.conn = nil
			ac.port++
			continue
		}
		return resp, nil
	}
}

func (ac *AuctionClient) Result() (*proto.ResultResponse, error) {
	for {
		if ac.client == nil {
			_ = ac.connect()
		}
		if ac.client == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := ac.client.Result(ctx, &proto.ResultRequest{BidderId: ac.id})
		cancel()
		if err != nil {
			if ac.conn != nil {
				_ = ac.conn.Close()
			}
			ac.client = nil
			ac.conn = nil
			ac.port++
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
	fmt.Println("Commands:")
	fmt.Println("  bid <amount>  - Place a bid")
	fmt.Println("  result        - Check auction status")
	fmt.Println("  quit          - Exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		parts := strings.Fields(text)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "bid":
			var amtStr string
			if len(parts) == 1 {
				fmt.Println("Usage: bid <amount>")
				continue
			}
			amtStr = parts[1]

			amt, err := strconv.Atoi(amtStr)
			if err != nil {
				fmt.Println("Invalid number")
				continue
			}

			resp, _ := client.Bid(int32(amt))
			switch resp.Status {
			case proto.BidResponse_SUCCESS:
				fmt.Printf("✓ %s\n", resp.Message)
			case proto.BidResponse_FAIL:
				fmt.Printf("✗ %s\n", resp.Message)
			case proto.BidResponse_EXCEPTION:
				fmt.Printf("⚠ %s\n", resp.Message)
			}

		case "result":
			resp, _ := client.Result()
			fmt.Println("________________________")
			if resp.AuctionOver {
				fmt.Println("  AUCTION ENDED")
				if resp.Winner != "" {
					fmt.Printf("  Winner: %s\n", resp.Winner)
					fmt.Printf("  Winning bid: %d\n", resp.HighestBid)
				} else {
					fmt.Println("  No winner (no bids)")
				}
			} else {
				fmt.Println("  AUCTION IN PROGRESS")
				if resp.HighestBid > 0 {
					fmt.Printf("  Highest bid: %d\n", resp.HighestBid)
					fmt.Printf("  Leader: %s\n", resp.Winner)
				} else {
					fmt.Println("  No bids yet")
				}
			}
			fmt.Println("________________________")

		case "quit", "exit":
			fmt.Println("Goodbye!")
			return

		default:
			// Try parsing as just a number (original behavior)
			amt, err := strconv.Atoi(cmd)
			if err != nil {
				fmt.Println("Unknown command.  Try: bid <amount>, result, quit")
				continue
			}
			resp, _ := client.Bid(int32(amt))
			switch resp.Status {
			case proto.BidResponse_SUCCESS:
				fmt.Printf("✓ %s\n", resp.Message)
			case proto.BidResponse_FAIL:
				fmt.Printf("✗ %s\n", resp.Message)
			case proto.BidResponse_EXCEPTION:
				fmt.Printf("⚠ %s\n", resp.Message)
			}
		}
	}
}
