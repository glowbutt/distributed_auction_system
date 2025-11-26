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

const (
	RequestTimeout = 5 * time.Second

	// How long we wait between connect attempts when scanning ports (keeps CPU low).
	connectBackoff = 250 * time.Millisecond

	// If you want a maximum port to wrap at (optional). Set to 0 to disable wrap.
	maxPort = 0 // e.g. 8090 to wrap after 8090; 0 = no wrap

	// If you'd like, limit the number of ports we'll scan before giving up.
	// Set to 0 to allow unlimited scanning (client will block until it finds a server).
	maxPortsToScan = 0
)

// AuctionClient keeps only a single active connection. On failure it increments the port,
// connects to the next port and retries. Internal failures are silent.
type AuctionClient struct {
	bidderID    string
	host        string
	currentPort int

	conn   *grpc.ClientConn
	client proto.AuctionClient
}

// NewAuctionClient creates client that will start connecting at host:startPort.
func NewAuctionClient(bidderID, host string, startPort int) (*AuctionClient, error) {
	ac := &AuctionClient{
		bidderID:    bidderID,
		host:        host,
		currentPort: startPort,
	}

	// Try an initial connect but don't print on failure; caller will block on first RPC until a server is reachable.
	_ = ac.connectCurrent()
	return ac, nil
}

// connectCurrent dials the current host:port and sets ac.client / ac.conn.
// It returns error but does NOT print/log anything.
func (ac *AuctionClient) connectCurrent() error {
	address := fmt.Sprintf("%s:%d", ac.host, ac.currentPort)

	// Close existing connection (if any) before dialing a new one.
	if ac.conn != nil {
		_ = ac.conn.Close()
		ac.conn = nil
		ac.client = nil
	}

	// Use grpc.Dial (non-blocking) with insecure creds as before.
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	ac.conn = conn
	ac.client = proto.NewAuctionClient(conn)
	// Successful connect: we don't print any error messages here, but it's safe to keep a light info print if desired.
	// fmt.Printf("Connected to server: %s\n", address) // <- optional
	return nil
}

// Close closes the active connection.
func (ac *AuctionClient) Close() {
	if ac.conn != nil {
		_ = ac.conn.Close()
		ac.conn = nil
	}
}

// incrementPort increments port by 1 and optionally wraps at maxPort.
// Returns the new port value.
func (ac *AuctionClient) incrementPort() int {
	ac.currentPort++
	if maxPort > 0 {
		if ac.currentPort > maxPort {
			// wrap to start at a reasonable bound (you can choose a minimum)
			// For simplicity, wrap to 1 if maxPort set improperly.
			ac.currentPort = 1
		}
	}
	return ac.currentPort
}

// ensureConnected blocks until a working connection is established. It silently scans ports,
// incrementing the port on each failed dial, and returns only after a successful dial.
//
// If maxPortsToScan > 0, it will stop after scanning that many ports and return an error.
func (ac *AuctionClient) ensureConnected() error {
	portsScanned := 0
	for {
		// If we already have a client, return immediately.
		if ac.client != nil && ac.conn != nil {
			return nil
		}

		// Try to connect to the current port
		if err := ac.connectCurrent(); err == nil {
			// connected successfully
			return nil
		}

		// Failed to connect: move to next port silently
		ac.incrementPort()
		portsScanned++
		if maxPortsToScan > 0 && portsScanned >= maxPortsToScan {
			return fmt.Errorf("scanned %d ports without success", portsScanned)
		}
		// small backoff to avoid busy-looping
		time.Sleep(connectBackoff)
	}
}

// Bid sends a bid and will block+retry until it has a working connection and receives a response.
// On connection/RPC failure it increments the port and silently retries.
func (ac *AuctionClient) Bid(amount int32) (*proto.BidResponse, error) {
	for {
		// Ensure we have a live connection (blocks until successful unless maxPortsToScan limits it)
		if err := ac.ensureConnected(); err != nil {
			// If ensureConnected returns an error (because of configured limits), propagate it to caller.
			return nil, err
		}

		// Do the RPC with a timeout, but treat failures as "server down" -> try next port and continue loop.
		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := ac.client.Bid(ctx, &proto.BidRequest{
			BidderId: ac.bidderID,
			Amount:   amount,
		})
		cancel()

		if err == nil {
			// success
			return resp, nil
		}

		// RPC failed: treat as server crash/unavailable. Close connection, increment port and retry.
		_ = ac.conn.Close()
		ac.conn = nil
		ac.client = nil

		// increment port silently and try to connect to the next one
		ac.incrementPort()
		// small sleep before next connect attempt
		time.Sleep(connectBackoff)
		continue
	}
}

// Result works same as Bid: block until a connection exists and the RPC succeeds.
func (ac *AuctionClient) Result() (*proto.ResultResponse, error) {
	for {
		if err := ac.ensureConnected(); err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		resp, err := ac.client.Result(ctx, &proto.ResultRequest{
			BidderId: ac.bidderID,
		})
		cancel()

		if err == nil {
			return resp, nil
		}

		// RPC failed -> assume server down, rotate port & retry
		_ = ac.conn.Close()
		ac.conn = nil
		ac.client = nil

		ac.incrementPort()
		time.Sleep(connectBackoff)
		continue
	}
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
	host := flag.String("host", "localhost", "Server host")
	startPort := flag.Int("startport", 8080, "Starting server port (will increment on timeout)")

	flag.Parse()

	if *bidderID == "" {
		fmt.Fprintln(os.Stderr, "Bidder ID is required. Use -id flag.")
		os.Exit(1)
	}

	client, err := NewAuctionClient(*bidderID, *host, *startPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Printf("\n=== Distributed Auction Client ===\n")
	fmt.Printf("Bidder ID: %s\n", *bidderID)
	fmt.Printf("Starting server: %s:%d\n", *host, *startPort)
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
				fmt.Printf("Error sending bid: %v\n", err)
			} else if resp.Status == proto.BidResponse_EXCEPTION {
				fmt.Println("Leader unavailable, try again later")
			} else {
				fmt.Println(resp.Message)
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
