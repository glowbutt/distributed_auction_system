package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	proto "auction-system/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	AuctionDuration     = 200 * time.Second // auction lasts
	ReplicationTimeout  = 10 * time.Second  // maximum time we wait for syncing states and forwarding
	electionCooldown    = 1 * time.Second   // throttle repeated elections
	dialQuickTimeout    = 2 * time.Second   // short dial timeout when trying to reach leader
	defaultHostFallback = "localhost"       // used when promoting self to leader address
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
	port          string // just the numeric port string like "8081"
	leaderAddress string // full address like "localhost:8081"
	peerAddresses []string
	peerConns     map[string]*grpc.ClientConn

	// Auction state
	bids           map[string]int32 // bidderID -> highest bid
	highestBid     int32
	highestBidder  string
	auctionEndTime time.Time
	sequenceNumber int64

	// election controls
	lastElection time.Time
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

	server := startServer(*nodeID, *isLeader, *leaderAddr, *port)
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
		server.connectToPeers(peerList)

		if !*isLeader {
			server.syncStateFromLeader()
		}
	}()

	log.Printf("[%s] Server starting on port %s (leader=%v)", *nodeID, *port, *isLeader)
	log.Printf("[%s] Auction will end at %v", *nodeID, server.auctionEndTime)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Failed to serve: %v", *nodeID, err)
	}
}

func startServer(nodeID string, isLeader bool, leaderAddr string, port string) *AuctionNode {
	return &AuctionNode{
		nodeID:         nodeID,
		isLeader:       isLeader,
		leaderAddress:  leaderAddr,
		port:           port,
		peerConns:      make(map[string]*grpc.ClientConn),
		bids:           make(map[string]int32),
		auctionEndTime: time.Now().Add(AuctionDuration),
	}
}

func (n *AuctionNode) connectToPeers(peers []string) {
	for _, addr := range peers {
		if addr == "" {
			continue
		}
		// Use Dial (non-blocking) so startup doesn't hang waiting for peers
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Failed to connect to peer %s: %v", n.nodeID, addr, err)
			continue
		}
		n.mutex.Lock()
		n.peerConns[addr] = conn
		n.mutex.Unlock()
		log.Printf("[%s] Connected to peer: %s", n.nodeID, addr)
	}
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

// makeNewLeader chooses a replacement leader, removes the old leader connection,
// and notifies all peers to update their local leaderAddress to newLeader.
// NOTE: this function performs the election action, but it only acts if the
// current leaderAddress still equals oldLeaderAddr (prevents stale elections).
func (n *AuctionNode) makeNewLeader(oldLeaderAddr string) {
	n.mutex.Lock()
	currentLeader := n.leaderAddress

	// Quick exit if leader already changed
	if currentLeader != oldLeaderAddr {
		n.mutex.Unlock()
		log.Printf("[%s] makeNewLeader: leader already different (current=%s, failed=%s) — skipping", n.nodeID, currentLeader, oldLeaderAddr)
		return
	}

	// Snapshot peers
	peers := make([]string, 0, len(n.peerConns))
	for addr := range n.peerConns {
		if addr != oldLeaderAddr {
			peers = append(peers, addr)
		}
	}
	n.mutex.Unlock()

	// Pick first live peer as new leader
	var newLeader string
	for _, addr := range peers {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(dialQuickTimeout))
		if err == nil && conn != nil {
			newLeader = addr
			conn.Close() // temporary check
			break
		}
	}

	// If no live peer, promote self
	if newLeader == "" {
		newLeader = fmt.Sprintf("%s:%s", defaultHostFallback, n.port)
	}

	// Remove old leader connection locally
	n.Bury(oldLeaderAddr)

	// Update local state
	n.mutex.Lock()
	old := n.leaderAddress
	n.leaderAddress = newLeader
	n.isLeader = (newLeader == fmt.Sprintf("%s:%s", defaultHostFallback, n.port))
	n.mutex.Unlock()

	log.Printf("[%s] makeNewLeader: local leader set to %s (was %s)", n.nodeID, newLeader, old)

	// Notify peers (best-effort)
	for _, addr := range peers {
		go func(peerAddr string) {
			n.mutex.Lock()
			conn, ok := n.peerConns[peerAddr]
			n.mutex.Unlock()
			if !ok {
				return
			}

			client := proto.NewReplicationClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
			defer cancel()
			_, err := client.MakeNewLeader(ctx, &proto.MakeNewLeaderRequest{Address: newLeader})
			if err != nil {
				log.Printf("[%s] makeNewLeader: failed to notify %s of new leader %s: %v", n.nodeID, peerAddr, newLeader, err)
			} else {
				log.Printf("[%s] makeNewLeader: notified %s to set leader=%s", n.nodeID, peerAddr, newLeader)
			}
		}(addr)
	}
}

func (n *AuctionNode) MakeNewLeader(ctx context.Context, req *proto.MakeNewLeaderRequest) (*proto.MakeNewLeaderResponse, error) {
	if req == nil || req.Address == "" {
		return &proto.MakeNewLeaderResponse{Success: false, Message: "empty new_address"}, nil
	}

	n.mutex.Lock()
	old := n.leaderAddress
	n.leaderAddress = req.Address
	n.isLeader = (n.leaderAddress == fmt.Sprintf("%s:%s", defaultHostFallback, n.port))
	n.mutex.Unlock()

	log.Printf("[%s] MakeNewLeader RPC: leader changed from %s to %s (requested)", n.nodeID, old, req.Address)

	// Optionally: proactively remove connection to old leader locally if present
	if old != "" && old != req.Address {
		n.mutex.Lock()
		if conn, ok := n.peerConns[old]; ok {
			_ = conn.Close()
			delete(n.peerConns, old)
			log.Printf("[%s] MakeNewLeader RPC: closed and removed old leader connection %s", n.nodeID, old)
		}
		n.mutex.Unlock()
	}

	return &proto.MakeNewLeaderResponse{Success: true, Message: "leader updated"}, nil
}

func (n *AuctionNode) Bury(address string) {
	if address == "" {
		return
	}

	// copy peers under lock
	n.mutex.Lock()
	peers := make(map[string]*grpc.ClientConn, len(n.peerConns))
	for a, c := range n.peerConns {
		peers[a] = c
	}
	n.mutex.Unlock()

	// Ask peers to remove the given address (best-effort). Fire-and-forget so we don't block.
	for addr, conn := range peers {
		go func(a string, c *grpc.ClientConn) {
			client := proto.NewReplicationClient(c)
			ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
			defer cancel()
			_, err := client.RemovePeer(ctx, &proto.RemovePeerRequest{Address: address})
			if err != nil {
				log.Printf("[%s] Bury: failed to notify %s to remove %s: %v", n.nodeID, a, address, err)
			} else {
				log.Printf("[%s] Bury: notified %s to remove %s", n.nodeID, a, address)
			}
		}(addr, conn)
	}

	// Remove local connection if present
	n.mutex.Lock()
	if conn, ok := n.peerConns[address]; ok {
		_ = conn.Close()
		delete(n.peerConns, address)
		log.Printf("[%s] Bury: locally removed %s", n.nodeID, address)
	} else {
		log.Printf("[%s] Bury: local peer %s not present", n.nodeID, address)
	}
	n.mutex.Unlock()
}

func (n *AuctionNode) RemovePeer(ctx context.Context, req *proto.RemovePeerRequest) (*proto.RemovePeerResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	addr := req.Address
	if addr == "" {
		return &proto.RemovePeerResponse{Success: false, Message: "empty address"}, nil
	}

	if conn, ok := n.peerConns[addr]; ok {
		// best-effort close
		_ = conn.Close()
		delete(n.peerConns, addr)
		log.Printf("[%s] Removed peer %s per remote request", n.nodeID, addr)
		return &proto.RemovePeerResponse{Success: true, Message: "removed"}, nil
	}
	return &proto.RemovePeerResponse{Success: false, Message: "peer not found"}, nil
}

func (n *AuctionNode) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	// Unlock mutex for network I/O
	n.mutex.Unlock()
	defer n.mutex.Lock() // relock before returning

	retryTimeout := 3 * time.Second
	deadline := time.Now().Add(retryTimeout)

	for {
		// Snapshot leader info
		n.mutex.Lock()
		leaderAddr := n.leaderAddress
		var leaderConn *grpc.ClientConn
		if c, ok := n.peerConns[leaderAddr]; ok {
			leaderConn = c
		}
		n.mutex.Unlock()

		if leaderAddr == "" {
			// No known leader — trigger election and wait a bit
			n.triggerElectionIfNeeded("")
			if time.Now().After(deadline) {
				return &proto.BidResponse{
					Status:  proto.BidResponse_EXCEPTION,
					Message: "No leader available",
				}, nil
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var auctionClient proto.AuctionClient
		var tempConn *grpc.ClientConn

		if leaderConn != nil {
			auctionClient = proto.NewAuctionClient(leaderConn)
		} else {
			// Try dialing leader
			dialCtx, dialCancel := context.WithTimeout(ctx, dialQuickTimeout)
			conn, err := grpc.DialContext(dialCtx, leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			dialCancel()
			if err != nil {
				// leader down, trigger election
				go n.triggerElectionIfNeeded(leaderAddr)
				if time.Now().After(deadline) {
					return &proto.BidResponse{
						Status:  proto.BidResponse_EXCEPTION,
						Message: fmt.Sprintf("Cannot reach leader: %v", err),
					}, nil
				}
				time.Sleep(200 * time.Millisecond)
				continue
			}
			tempConn = conn
			auctionClient = proto.NewAuctionClient(conn)
		}

		// Call Bid RPC
		rpcCtx, rpcCancel := context.WithTimeout(ctx, ReplicationTimeout)
		resp, err := auctionClient.Bid(rpcCtx, req)
		rpcCancel()

		if tempConn != nil {
			_ = tempConn.Close()
		}

		if err != nil {
			log.Printf("[%s] forwardBidToLeader: leader %s failed: %v", n.nodeID, leaderAddr, err)
			go n.triggerElectionIfNeeded(leaderAddr)
			if time.Now().After(deadline) {
				return &proto.BidResponse{
					Status:  proto.BidResponse_EXCEPTION,
					Message: fmt.Sprintf("Leader communication failed: %v", err),
				}, nil
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// Success
		return resp, nil
	}
}

// triggerElectionIfNeeded checks & throttles election launches and then calls makeNewLeader in a goroutine.
func (n *AuctionNode) triggerElectionIfNeeded(failedLeader string) {
	n.mutex.Lock()
	// If leaderAddress already differs, don't start election
	if n.leaderAddress != failedLeader {
		n.mutex.Unlock()
		return
	}
	// If an election ran recently, skip
	if time.Since(n.lastElection) < electionCooldown {
		n.mutex.Unlock()
		return
	}
	// mark lastElection now so other attempts will be throttled
	n.lastElection = time.Now()
	n.mutex.Unlock()

	// run election asynchronously (non-blocking)
	go n.makeNewLeader(failedLeader)
}

func (n *AuctionNode) processBid(req *proto.BidRequest) (*proto.BidResponse, error) {
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

	// replicate data to followers (best-effort)
	go n.replicateToFollowers(req.BidderId, req.Amount, timestamp, n.sequenceNumber)

	return &proto.BidResponse{
		Status:  proto.BidResponse_SUCCESS,
		Message: fmt.Sprintf("Bid of %d accepted", req.Amount),
	}, nil
}

func (n *AuctionNode) replicateToFollowers(bidderID string, amount int32, timestamp, seqNum int64) (isMajority bool, numOfSuccesses int) {
	replicateReq := &proto.ReplicateBidRequest{
		BidderId:       bidderID,
		Amount:         amount,
		Timestamp:      timestamp,
		SequenceNumber: seqNum,
	}

	isMajority = false
	successCount := 1 // count self

	// copy peerConns under lock
	n.mutex.Lock()
	peers := make(map[string]*grpc.ClientConn, len(n.peerConns))
	for a, c := range n.peerConns {
		peers[a] = c
	}
	n.mutex.Unlock()

	for addr, c := range peers {
		client := proto.NewReplicationClient(c)
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

	// log whether majority has replicated or not. Asynchronous
	majority := (len(peers) + 2) / 2
	if successCount >= majority {
		log.Printf("[%s] Bid replicated to majority (%d/%d)", n.nodeID, successCount, len(peers)+1)
		isMajority = true
	} else {
		log.Printf("[%s] Warning: Bid only replicated to %d/%d nodes", n.nodeID, successCount, len(peers)+1)
	}
	return isMajority, successCount
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

	n.bids[req.BidderId] = req.Amount
	if req.Amount > n.highestBid {
		n.highestBid = req.Amount
		n.highestBidder = req.BidderId
	}
	n.sequenceNumber = req.SequenceNumber

	return &proto.ReplicateBidResponse{Success: true}, nil
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

func (n *AuctionNode) syncStateFromLeader() {
	if n.isLeader || n.leaderAddress == "" {
		return
	}

	// connect to leader
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
	for _, bid := range state.Bids {
		n.bids[bid.BidderId] = bid.Amount
		if bid.Amount > n.highestBid {
			n.highestBid = bid.Amount
			n.highestBidder = bid.BidderId
		}
	}
	n.sequenceNumber = state.SequenceNumber
	n.auctionEndTime = time.Unix(0, state.AuctionEndTime)
	n.mutex.Unlock()

	log.Printf("[%s] Synchronized state from leader: %d bids, seq=%d",
		n.nodeID, len(state.Bids), state.SequenceNumber)
}

func (n *AuctionNode) Close() {
	n.mutex.Lock()
	for _, conn := range n.peerConns {
		conn.Close()
	}
	n.mutex.Unlock()
}
