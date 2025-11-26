package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	proto "auction-system/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	AuctionDuration    = 100 * time.Second //auction lasts
	HeartbeatInterval  = 10 * time.Second  // nodes send heartbeat every 10 seconds
	HeartbeatTimeout   = 15 * time.Second  // accepted timeframe with no heartbeat
	ReplicationTimeout = 3 * time.Second   //??
)

type BidEntry struct {
	BidderID  string
	Amount    int32
	Timestamp int64
}

type AuctionNode struct {
	proto.UnimplementedAuctionServer
	proto.UnimplementedReplicationServer

	mutex         sync.Mutex
	nodeID        string
	isLeader      bool
	leaderAddress string
	peerAddresses []string
	peerClients   map[string]proto.ReplicationClient
	peerConns     map[string]*grpc.ClientConn

	// Auction state
	bids           map[string]int32 // bidderID -> highest bid
	highestBid     int32
	highestBidder  string
	auctionEndTime time.Time
	sequenceNumber int64

	// For tracking registered bidders
	registeredBidders map[string]bool
}

func main() {
	nodeID := flag.String("id", "node1", "Node ID")
	port := flag.String("port", "8080", "Port to listen on")
	isLeader := flag.Bool("leader", false, "Is this node the leader")
	leaderAddr := flag.String("leader-addr", "", "Leader address (for followers)")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses")

	flag.Parse()

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	server := startServer(*nodeID, *isLeader, *leaderAddr, peerList)

	// Create gRPC server
	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("[%s] Failed to listen: %v", *nodeID, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterAuctionServer(grpcServer, server)
	proto.RegisterReplicationServer(grpcServer, server)

	// Give time for other nodes to start, then connect
	go func() {
		time.Sleep(2 * time.Second)
		server.connectToPeers()

		if !*isLeader {
			server.syncStateFromLeader()
		}

		server.startHeartbeat()
	}()

	log.Printf("[%s] Server starting on port %s (leader=%v)", *nodeID, *port, *isLeader)
	log.Printf("[%s] Auction will end at %v", *nodeID, server.auctionEndTime)

	// handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Printf("[%s] Shutting down...", *nodeID)
		server.Close()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Failed to serve: %v", *nodeID, err)
	}
}

func startServer(nodeID string, isLeader bool, leaderAddr string, peers []string) *AuctionNode {
	return &AuctionNode{
		nodeID:            nodeID,
		isLeader:          isLeader,
		leaderAddress:     leaderAddr,
		peerAddresses:     peers,
		peerClients:       make(map[string]proto.ReplicationClient),
		peerConns:         make(map[string]*grpc.ClientConn),
		bids:              make(map[string]int32),
		registeredBidders: make(map[string]bool),
		auctionEndTime:    time.Now().Add(AuctionDuration),
	}
}

func (n *AuctionNode) connectToPeers() {
	for _, addr := range n.peerAddresses {
		if addr == "" {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Failed to connect to peer %s: %v", n.nodeID, addr, err)
			continue
		}
		n.peerConns[addr] = conn
		n.peerClients[addr] = proto.NewReplicationClient(conn)
		log.Printf("[%s] Connected to peer: %s", n.nodeID, addr)
	}
}

func (n *AuctionNode) connectToLeader() proto.ReplicationClient {
	if n.leaderAddress == "" {
		return nil
	}
	conn, err := grpc.Dial(n.leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] Failed to connect to leader %s: %v", n.nodeID, n.leaderAddress, err)
		return nil
	}
	return proto.NewReplicationClient(conn)
}

// Bid handles bid requests from clients
func (n *AuctionNode) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	log.Printf("[%s] Received bid: bidder=%s, amount=%d", n.nodeID, req.BidderId, req.Amount)

	// check if auction is over
	if time.Now().After(n.auctionEndTime) {
		return &proto.BidResponse{
			Status:  proto.BidResponse_FAIL,
			Message: "Auction has ended",
		}, nil
	}

	// if not leader, forward to leader
	if !n.isLeader {
		return n.forwardBidToLeader(ctx, req)
	}

	// leader processes the bid
	return n.processBid(req)
}

func (n *AuctionNode) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	n.mutex.Unlock() // we unlock while forwarding

	client := n.connectToLeader()
	if client == nil {
		n.mutex.Lock()
		return &proto.BidResponse{
			Status:  proto.BidResponse_EXCEPTION,
			Message: "Cannot reach leader",
		}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, ReplicationTimeout)
	defer cancel()

	resp, err := proto.NewAuctionClient(n.peerConns[n.leaderAddress]).Bid(ctx, req)
	n.mutex.Lock()

	if err != nil {
		return &proto.BidResponse{
			Status:  proto.BidResponse_EXCEPTION,
			Message: fmt.Sprintf("Leader communication failed: %v", err),
		}, nil
	}
	return resp, nil
}

func (n *AuctionNode) processBid(req *proto.BidRequest) (*proto.BidResponse, error) {
	// First bid registers the bidder
	if !n.registeredBidders[req.BidderId] {
		n.registeredBidders[req.BidderId] = true
		log.Printf("[%s] Registered new bidder: %s", n.nodeID, req.BidderId)
	}

	// Check if bid is higher than bidder's previous bid
	if prevBid, exists := n.bids[req.BidderId]; exists && req.Amount <= prevBid {
		return &proto.BidResponse{
			Status:  proto.BidResponse_FAIL,
			Message: fmt.Sprintf("Bid must be higher than your previous bid of %d", prevBid),
		}, nil
	}

	// Check if bid is higher than current highest
	if req.Amount <= n.highestBid {
		return &proto.BidResponse{
			Status:  proto.BidResponse_FAIL,
			Message: fmt.Sprintf("Bid must be higher than current highest bid of %d", n.highestBid),
		}, nil
	}

	// Update state
	n.sequenceNumber++
	n.bids[req.BidderId] = req.Amount
	n.highestBid = req.Amount
	n.highestBidder = req.BidderId
	timestamp := time.Now().UnixNano()

	// replicate data to followers
	n.replicateToFollowers(req.BidderId, req.Amount, timestamp, n.sequenceNumber)

	return &proto.BidResponse{
		Status:  proto.BidResponse_SUCCESS,
		Message: fmt.Sprintf("Bid of %d accepted", req.Amount),
	}, nil
}

func (n *AuctionNode) replicateToFollowers(bidderID string, amount int32, timestamp, seqNum int64) {
	replicateReq := &proto.ReplicateBidRequest{
		BidderId:       bidderID,
		Amount:         amount,
		Timestamp:      timestamp,
		SequenceNumber: seqNum,
	}

	successCount := 1 // count self

	for addr, client := range n.peerClients {
		ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		resp, err := client.ReplicateBid(ctx, replicateReq)
		cancel()

		if err != nil {
			log.Printf("[%s] Failed to replicate to %s: %v", n.nodeID, addr, err)
			continue
		}
		if resp.Success {
			successCount++
		}
	}

	// Need majority for commit (with 3 nodes, need 2)
	majority := (len(n.peerClients) + 2) / 2
	if successCount >= majority {
		log.Printf("[%s] Bid replicated to majority (%d/%d)", n.nodeID, successCount, len(n.peerClients)+1)
	} else {
		log.Printf("[%s] Warning: Bid only replicated to %d/%d nodes", n.nodeID, successCount, len(n.peerClients)+1)
	}
}

// Result returns the current state or winner of the auction
func (n *AuctionNode) Result(ctx context.Context, req *proto.ResultRequest) (*proto.ResultResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	log.Printf("[%s] Result query from %s", n.nodeID, req.BidderId)

	auctionOver := time.Now().After(n.auctionEndTime)

	if auctionOver {
		if n.highestBidder == "" {
			return &proto.ResultResponse{
				AuctionOver: true,
				HighestBid:  0,
				Message:     "Auction ended with no bids",
			}, nil
		}
		return &proto.ResultResponse{
			AuctionOver: true,
			Winner:      n.highestBidder,
			HighestBid:  n.highestBid,
			Message:     fmt.Sprintf("Auction ended. Winner: %s with bid of %d", n.highestBidder, n.highestBid),
		}, nil
	}

	return &proto.ResultResponse{
		AuctionOver: false,
		HighestBid:  n.highestBid,
		Winner:      n.highestBidder,
		Message:     fmt.Sprintf("Auction ongoing. Current highest bid: %d by %s", n.highestBid, n.highestBidder),
	}, nil
}

// ReplicateBid handles replication requests from the leader
func (n *AuctionNode) ReplicateBid(ctx context.Context, req *proto.ReplicateBidRequest) (*proto.ReplicateBidResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	log.Printf("[%s] Replicating bid: bidder=%s, amount=%d, seq=%d",
		n.nodeID, req.BidderId, req.Amount, req.SequenceNumber)

	// Apply the bid
	if !n.registeredBidders[req.BidderId] {
		n.registeredBidders[req.BidderId] = true
	}

	n.bids[req.BidderId] = req.Amount
	if req.Amount > n.highestBid {
		n.highestBid = req.Amount
		n.highestBidder = req.BidderId
	}
	n.sequenceNumber = req.SequenceNumber

	return &proto.ReplicateBidResponse{Success: true}, nil
}

// Heartbeat handles heartbeat messages from leader
func (n *AuctionNode) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	log.Printf("[%s] Received heartbeat from leader %s", n.nodeID, req.LeaderId)
	return &proto.HeartbeatResponse{
		Acknowledged: true,
		NodeId:       n.nodeID,
	}, nil
}

// GetState returns the current state for synchronization
func (n *AuctionNode) GetState(ctx context.Context, req *proto.GetStateRequest) (*proto.GetStateResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	bids := make([]*proto.BidEntry, 0, len(n.bids))
	for bidderID, amount := range n.bids {
		bids = append(bids, &proto.BidEntry{
			BidderId: bidderID,
			Amount:   amount,
		})
	}

	return &proto.GetStateResponse{
		Bids:           bids,
		AuctionEndTime: n.auctionEndTime.UnixNano(),
		SequenceNumber: n.sequenceNumber,
	}, nil
}

func (n *AuctionNode) startHeartbeat() {
	if !n.isLeader {
		return
	}

	ticker := time.NewTicker(HeartbeatInterval) //ticker is like an alarm that goes off repeatedly
	go func() {
		for range ticker.C { // every time there is a tick, send heartbeat to all followers
			n.mutex.Lock()
			for addr, client := range n.peerClients {
				ctx, cancel := context.WithTimeout(context.Background(), HeartbeatTimeout) //5 second timer
				_, err := client.Heartbeat(ctx, &proto.HeartbeatRequest{
					LeaderId: n.nodeID,
					Term:     1,
				})
				cancel()
				if err != nil {
					log.Printf("[%s] Heartbeat to %s failed: %v", n.nodeID, addr, err)
				}
			}
			n.mutex.Unlock()
		}
	}()
}

func (n *AuctionNode) syncStateFromLeader() {
	if n.isLeader || n.leaderAddress == "" {
		return
	}
	//dial connects to another server over the network :-)
	conn, err := grpc.Dial(n.leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] Cannot sync from leader: %v", n.nodeID, err)
		return
	}
	defer conn.Close()

	client := proto.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
	defer cancel()

	state, err := client.GetState(ctx, &proto.GetStateRequest{RequesterId: n.nodeID})
	if err != nil {
		log.Printf("[%s] Failed to get state from leader: %v", n.nodeID, err)
		return
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	for _, bid := range state.Bids {
		n.registeredBidders[bid.BidderId] = true
		n.bids[bid.BidderId] = bid.Amount
		if bid.Amount > n.highestBid {
			n.highestBid = bid.Amount
			n.highestBidder = bid.BidderId
		}
	}
	n.sequenceNumber = state.SequenceNumber
	n.auctionEndTime = time.Unix(0, state.AuctionEndTime)

	log.Printf("[%s] Synchronized state from leader: %d bids, seq=%d",
		n.nodeID, len(state.Bids), state.SequenceNumber)
}

func (n *AuctionNode) Close() {
	for _, conn := range n.peerConns {
		conn.Close()
	}
}
