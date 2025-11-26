package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	proto "auction-system/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	RequestTimeout = 5 * time.Second
)

type AuctionClient struct {
	bidderID    string
	serverAddrs []string
	clients     []proto.AuctionClient
	conns       []*grpc.ClientConn
	currentIdx  int
}

func NewAuctionClient(bidderID string, serverAddress []string) (*AuctionClient, error) {
	client := &AuctionClient{
		bidderID:    bidderID,
		serverAddrs: serverAddress,
		clients:     make([]proto.AuctionClient, len(serverAddress)),
		conns:       make([]*grpc.ClientConn, len(serverAddress)),
	}

	for i, addr := range serverAddress {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Warning: Failed to connect to %s: %v", addr, err)
			continue
		}
		client.conns[i] = conn
		client.clients[i] = proto.NewAuctionClient(conn)
		log.Printf("Connected to server: %s", addr)
	}

	return client, nil
}

func (c *AuctionClient) Close() {
	for _, conn := range c.conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func (c *AuctionClient) getNextClient() (proto.AuctionClient, string) {
	// Try all servers starting from current index
	for i := 0; i < len(c.clients); i++ {
		idx := (c.currentIdx + i) % len(c.clients)
		if c.clients[idx] != nil {
			c.currentIdx = (idx + 1) % len(c.clients)
			return c.clients[idx], c.serverAddrs[idx]
		}
	}
	return nil, ""
}

func (c *AuctionClient) Bid(amount int32) (*proto.BidResponse, error) {
	for attempts := 0; attempts < len(c.clients); attempts++ {
		client, addr := c.getNextClient()
		if client == nil {
			return nil, fmt.Errorf("no available servers")
		}

		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := client.Bid(ctx, &proto.BidRequest{
			BidderId: c.bidderID,
			Amount:   amount,
		})
		cancel()

		if err != nil {
			log.Printf("Bid failed on %s: %v, trying next server...", addr, err)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("all servers failed")
}

func (c *AuctionClient) Result() (*proto.ResultResponse, error) {
	for attempts := 0; attempts < len(c.clients); attempts++ {
		client, addr := c.getNextClient()
		if client == nil {
			return nil, fmt.Errorf("no available servers")
		}

		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := client.Result(ctx, &proto.ResultRequest{
			BidderId: c.bidderID,
		})
		cancel()

		if err != nil {
			log.Printf("Result query failed on %s: %v, trying next server...", addr, err)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("all servers failed")
}

func printHelp() {
	fmt.Println("\nAvailable commands:")
	fmt.Println("  bid <amount>  - Place a bid with the specified amount")
	fmt.Println("  result        - Query the current auction state")
	fmt.Println("  help          - Show this help message")
	fmt.Println("  quit/exit     - Exit the client")
	fmt.Println()
}

func main() {
	bidderID := flag.String("id", "", "Bidder ID (required)")
	servers := flag.String("servers", "localhost:8080,localhost:8081,localhost:8082",
		"Comma-separated list of server addresses")

	flag.Parse()

	if *bidderID == "" {
		log.Fatal("Bidder ID is required. Use -id flag.")
	}

	serverList := strings.Split(*servers, ",")

	client, err := NewAuctionClient(*bidderID, serverList)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Printf("\n=== Distributed Auction Client ===\n")
	fmt.Printf("Bidder ID: %s\n", *bidderID)
	fmt.Printf("Connected to servers: %v\n", serverList)
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := strings.ToLower(parts[0])

		switch command {
		case "bid":
			if len(parts) < 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}
			amount, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("Invalid amount. Please enter a number.")
				continue
			}

			resp, err := client.Bid(int32(amount))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			switch resp.Status {
			case proto.BidResponse_SUCCESS:
				fmt.Printf("✓ SUCCESS: %s\n", resp.Message)
			case proto.BidResponse_FAIL:
				fmt.Printf("✗ FAIL: %s\n", resp.Message)
			case proto.BidResponse_EXCEPTION:
				fmt.Printf("⚠ EXCEPTION: %s\n", resp.Message)
			}

		case "result":
			resp, err := client.Result()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			if resp.AuctionOver {
				fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				fmt.Println("  AUCTION ENDED")
				if resp.Winner != "" {
					fmt.Printf("  Winner: %s\n", resp.Winner)
					fmt.Printf("  Winning Bid: %d\n", resp.HighestBid)
				} else {
					fmt.Println("  No winner (no bids placed)")
				}
				fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			} else {
				fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				fmt.Println("  AUCTION IN PROGRESS")
				if resp.HighestBid > 0 {
					fmt.Printf("  Highest Bid: %d\n", resp.HighestBid)
					fmt.Printf("  Current Leader: %s\n", resp.Winner)
				} else {
					fmt.Println("  No bids yet")
				}
				fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			}

		case "help":
			printHelp()

		case "quit", "exit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s\n", command)
			printHelp()
		}
	}
}
