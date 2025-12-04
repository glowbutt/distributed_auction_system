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
	AuctionDuration    = 200 * time.Second
	ReplicationTimeout = 10 * time.Second
	QuickTimeout       = 2 * time.Second
	electionCooldown   = 1 * time.Second
	hostFallback       = "localhost"
)

type AuctionNode struct {
	proto.UnimplementedAuctionServer
	proto.UnimplementedReplicationServer

	mutex         sync.Mutex
	nodeID        string
	isLeader      bool
	port          string
	leaderAddress string
	peerConns     map[string]*grpc.ClientConn
	bids          map[string]int32
	highestBid    int32
	highestBidder string
	auctionEnd    time.Time
	seq           int64
	lastElection  time.Time

	// election state
	term     int64  // current term
	votedFor string // candidate this node voted for in current term
}

func main() {
	id := flag.String("id", "node1", "")
	port := flag.String("port", "8080", "")
	leader := flag.Bool("leader", false, "")
	leaderAddr := flag.String("leader-addr", "", "")
	peers := flag.String("peers", "", "")
	flag.Parse()

	pl := []string{}
	if *peers != "" {
		pl = strings.Split(*peers, ",")
	}

	n := &AuctionNode{
		nodeID:        *id,
		isLeader:      *leader,
		port:          *port,
		leaderAddress: *leaderAddr,
		peerConns:     map[string]*grpc.ClientConn{},
		bids:          map[string]int32{},
		auctionEnd:    time.Now().Add(AuctionDuration),
		term:          0,
		votedFor:      "",
	}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterAuctionServer(s, n)
	proto.RegisterReplicationServer(s, n)

	go func() {
		time.Sleep(2 * time.Second)
		n.connectToPeers(pl)
	}()

	log.Printf("[%s] start port=%s leader=%v", n.nodeID, n.port, n.isLeader)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// connectToPeers: best-effort initial connects; stores nil entries for peers
func (n *AuctionNode) connectToPeers(peers []string) {
	for _, a := range peers {
		if a == "" {
			continue
		}
		n.mutex.Lock()
		if _, ok := n.peerConns[a]; !ok {
			n.peerConns[a] = nil
		}
		n.mutex.Unlock()

		// async connect
		go func(addr string) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] initial connect to %s failed: %v", n.nodeID, addr, err)
				return
			}
			n.mutex.Lock()
			n.peerConns[addr] = conn
			n.mutex.Unlock()
			log.Printf("[%s] connected %s", n.nodeID, addr)
		}(a)
	}
}

// Bid RPC
func (n *AuctionNode) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	n.mutex.Lock()
	ended := time.Now().After(n.auctionEnd)
	if ended {
		n.mutex.Unlock()
		return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: "auction ended"}, nil
	}
	if !n.isLeader {
		n.mutex.Unlock()
		return n.forwardBidToLeader(ctx, req)
	}
	n.mutex.Unlock()
	return n.processBid(ctx, req)
}

// forwardBidToLeader forwards to the node in n.leaderAddress
// If leader missing/unreachable it triggers an election and retries until deadline
func (n *AuctionNode) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	deadline := time.Now().Add(3 * time.Second)
	for {
		n.mutex.Lock()
		leader := n.leaderAddress
		conn := n.peerConns[leader]
		n.mutex.Unlock()

		if leader == "" {
			n.triggerElectionIfNeeded("")
			if time.Now().After(deadline) {
				return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader unreachable"}, nil
			}
			time.Sleep(150 * time.Millisecond)
			continue
		}

		if conn == nil {
			connTmp, err := grpc.NewClient(leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// leader seems down; trigger election and retry
				go n.triggerElectionIfNeeded(leader)
				if time.Now().After(deadline) {
					return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader unreachable"}, nil
				}
				time.Sleep(150 * time.Millisecond)
				continue
			}
			n.mutex.Lock()
			n.peerConns[leader] = connTmp
			n.mutex.Unlock()
			conn = connTmp
		}

		client := proto.NewAuctionClient(conn)
		rctx, cancel := context.WithTimeout(ctx, ReplicationTimeout)
		resp, err := client.Bid(rctx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		// remote failed: trigger election and retry
		go n.triggerElectionIfNeeded(leader)
		if time.Now().After(deadline) {
			return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader not reached"}, nil
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// triggerElectionIfNeeded: avoids frequent elections and starts one when appropriate
func (n *AuctionNode) triggerElectionIfNeeded(failed string) {
	n.mutex.Lock()
	shouldStart := (n.leaderAddress == failed) && time.Since(n.lastElection) >= electionCooldown
	if shouldStart {
		n.lastElection = time.Now()
	}
	n.mutex.Unlock()
	if shouldStart {
		go n.startElection()
	}
}

// Result RPC
func (n *AuctionNode) Result(ctx context.Context, req *proto.ResultRequest) (*proto.ResultResponse, error) {
	n.mutex.Lock()
	if !n.isLeader {
		leader := n.leaderAddress
		conn := n.peerConns[leader]
		n.mutex.Unlock()
		if leader == "" {
			return &proto.ResultResponse{AuctionOver: false, HighestBid: 0, Message: "No leader known"}, nil
		}
		if conn == nil {
			var err error
			conn, err = grpc.NewClient(leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return &proto.ResultResponse{AuctionOver: false, HighestBid: 0, Message: fmt.Sprintf("Failed to connect to leader %s: %v", leader, err)}, nil
			}
			n.mutex.Lock()
			n.peerConns[leader] = conn
			n.mutex.Unlock()
		}
		client := proto.NewAuctionClient(conn)
		rctx, cancel := context.WithTimeout(ctx, ReplicationTimeout)
		defer cancel()
		return client.Result(rctx, req)
	}
	defer n.mutex.Unlock()

	log.Printf("[%s] Result query from %s", n.nodeID, req.BidderId)
	auctionOver := time.Now().After(n.auctionEnd)

	if auctionOver {
		if n.highestBidder == "" {
			return &proto.ResultResponse{AuctionOver: true, HighestBid: 0, Message: "Auction ended with no bids"}, nil
		}
		return &proto.ResultResponse{AuctionOver: true, Winner: n.highestBidder, HighestBid: n.highestBid, Message: fmt.Sprintf("Auction ended. Winner: %s with %d", n.highestBidder, n.highestBid)}, nil
	}

	if n.highestBidder == "" {
		return &proto.ResultResponse{AuctionOver: false, HighestBid: 0, Message: "Auction ongoing. No bids yet"}, nil
	}

	return &proto.ResultResponse{AuctionOver: false, Winner: n.highestBidder, HighestBid: n.highestBid, Message: fmt.Sprintf("Auction ongoing. Highest bid: %d by %s", n.highestBid, n.highestBidder)}, nil
}

// startElection implements an election and becomes leader only if majority votes
// should prevent split-brain by requiring majority
func (n *AuctionNode) startElection() {
	// Snapshot peers & state
	n.mutex.Lock()
	n.term++
	term := n.term
	selfAddr := fmt.Sprintf("%s:%s", hostFallback, n.port)
	n.votedFor = selfAddr
	n.lastElection = time.Now()

	peers := make([]string, 0, len(n.peerConns))
	for a := range n.peerConns {
		peers = append(peers, a)
	}
	mySeq := n.seq
	n.mutex.Unlock()

	totalNodes := len(peers) + 1
	majority := totalNodes/2 + 1

	// Become leader if only one node left
	if totalNodes == 1 || len(peers) == 0 {
		n.mutex.Lock()
		n.leaderAddress = selfAddr
		n.isLeader = true
		n.votedFor = "" // clear votedFor once leader
		n.mutex.Unlock()
		log.Printf("[%s] only one node left -> auto leader %s", n.nodeID, selfAddr)
		return
	}

	log.Printf("[%s] starting election term=%d peers=%d need=%d", n.nodeID, term, len(peers), majority)

	votes := 1 // vote for self

	for _, addr := range peers {
		// prefer stored connection
		n.mutex.Lock()
		stored := n.peerConns[addr]
		n.mutex.Unlock()

		var conn *grpc.ClientConn
		var oneOff bool
		if stored != nil {
			conn = stored
		} else {
			c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] RequestVote: connect %s failed: %v", n.nodeID, addr, err)
				continue
			}
			conn = c
			oneOff = true
		}

		rctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		resp, err := proto.NewReplicationClient(conn).RequestVote(rctx, &proto.RequestVoteRequest{
			Term:        term,
			CandidateId: selfAddr,
			LastSeq:     mySeq,
		})
		cancel()

		if oneOff {
			_ = conn.Close()
		}

		if err != nil {
			log.Printf("[%s] RequestVote RPC to %s failed: %v", n.nodeID, addr, err)
			continue
		}

		// If peer has higher term, step down
		n.mutex.Lock()
		if resp.Term > n.term {
			log.Printf("[%s] discovered higher term %d from %s (was %d), stepping down from election", n.nodeID, resp.Term, addr, n.term)
			n.term = resp.Term
			n.votedFor = ""
			n.isLeader = false
			n.mutex.Unlock()
			return
		}
		n.mutex.Unlock()

		if resp.VoteGranted {
			votes++
			log.Printf("[%s] vote from %s (votes=%d/%d)", n.nodeID, addr, votes, totalNodes)
			if votes >= majority {
				break
			}
		}
	}

	if votes < majority {
		log.Printf("[%s] election lost term=%d votes=%d/%d", n.nodeID, term, votes, totalNodes)
		return
	}

	// won election: become leader
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = selfAddr
	n.isLeader = true
	n.votedFor = "" // clear votedFor once leader
	n.mutex.Unlock()

	log.Printf("[%s] WON election term=%d votes=%d/%d -> leader=%s", n.nodeID, term, votes, totalNodes, selfAddr)

	// notifying peers
	for _, addr := range peers {
		n.mutex.Lock()
		stored := n.peerConns[addr]
		n.mutex.Unlock()

		var conn *grpc.ClientConn
		var oneOff bool
		if stored != nil {
			conn = stored
		} else {
			c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] notify %s failed connect: %v", n.nodeID, addr, err)
				continue
			}
			conn = c
			oneOff = true
		}

		rctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		_, err := proto.NewReplicationClient(conn).MakeNewLeader(rctx, &proto.MakeNewLeaderRequest{Address: selfAddr})
		cancel()
		if oneOff {
			_ = conn.Close()
		}
		if err != nil {
			log.Printf("[%s] notify %s MakeNewLeader RPC failed: %v", n.nodeID, addr, err)
			continue
		}
		log.Printf("[%s] told %s -> leader=%s", n.nodeID, addr, selfAddr)
	}

	log.Printf("[%s] leader changed %s -> %s", n.nodeID, prev, n.leaderAddress)
}

// MakeNewLeader RPC: follower updates leader info
func (n *AuctionNode) MakeNewLeader(ctx context.Context, req *proto.MakeNewLeaderRequest) (*proto.MakeNewLeaderResponse, error) {
	if req == nil || req.Address == "" {
		return &proto.MakeNewLeaderResponse{Success: false, Message: "empty"}, nil
	}
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = req.Address
	n.isLeader = (n.leaderAddress == fmt.Sprintf("%s:%s", hostFallback, n.port))
	n.votedFor = ""
	n.mutex.Unlock()
	log.Printf("[%s] received MakeNewLeader -> leader=%s (prev=%s)", n.nodeID, req.Address, prev)
	return &proto.MakeNewLeaderResponse{Success: true, Message: "ok"}, nil
}

// RequestVote RPC handler
func (n *AuctionNode) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	if req == nil {
		n.mutex.Lock()
		t := n.term
		n.mutex.Unlock()
		return &proto.RequestVoteResponse{Term: t, VoteGranted: false}, nil
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// reject old term
	if req.Term < n.term {
		return &proto.RequestVoteResponse{Term: n.term, VoteGranted: false}, nil
	}

	// use higher term
	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
		n.isLeader = false
	}

	// grant if not yet voted (or voted for this candidate) and candidate's log is at least as up-to-date (seq)
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		if req.LastSeq >= n.seq {
			n.votedFor = req.CandidateId
			n.lastElection = time.Now()
			log.Printf("[%s] granting vote to %s for term=%d (our seq=%d their seq=%d)", n.nodeID, req.CandidateId, req.Term, n.seq, req.LastSeq)
			return &proto.RequestVoteResponse{Term: n.term, VoteGranted: true}, nil
		}
	}

	return &proto.RequestVoteResponse{Term: n.term, VoteGranted: false}, nil
}

// Bury: request peers to remove an address (best-effort)
func (n *AuctionNode) Bury(addr string) {
	if addr == "" {
		return
	}
	n.mutex.Lock()
	conns := make(map[string]*grpc.ClientConn, len(n.peerConns))
	for a, c := range n.peerConns {
		conns[a] = c
	}
	n.mutex.Unlock()

	for a, c := range conns {
		go func(cc *grpc.ClientConn, peer string) {
			client := proto.NewReplicationClient(cc)
			ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
			defer cancel()
			_, _ = client.RemovePeer(ctx, &proto.RemovePeerRequest{Address: addr})
		}(c, a)
	}

	n.mutex.Lock()
	if c, ok := n.peerConns[addr]; ok {
		_ = c.Close()
		delete(n.peerConns, addr)
	}
	n.mutex.Unlock()
}

// RemovePeer RPC
func (n *AuctionNode) RemovePeer(ctx context.Context, req *proto.RemovePeerRequest) (*proto.RemovePeerResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if req == nil || req.Address == "" {
		return &proto.RemovePeerResponse{Success: false, Message: "empty"}, nil
	}
	if c, ok := n.peerConns[req.Address]; ok {
		_ = c.Close()
		delete(n.peerConns, req.Address)
		return &proto.RemovePeerResponse{Success: true, Message: "removed"}, nil
	}
	return &proto.RemovePeerResponse{Success: false, Message: "notfound"}, nil
}

// processBid replicates synchronously to a majority before confirming to client
func (n *AuctionNode) processBid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	// validate and apply under lock
	n.mutex.Lock()
	if prev, ok := n.bids[req.BidderId]; ok && req.Amount <= prev {
		n.mutex.Unlock()
		return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: fmt.Sprintf("must be higher than %d", prev)}, nil
	}
	if req.Amount <= n.highestBid {
		n.mutex.Unlock()
		return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: fmt.Sprintf("must be higher than %d", n.highestBid)}, nil
	}

	prevSeq := n.seq
	prevHighest := n.highestBid
	prevHighestBidder := n.highestBidder
	prevBidValue, hadPrevBid := n.bids[req.BidderId]

	n.seq++
	seq := n.seq
	n.bids[req.BidderId] = req.Amount
	n.highestBid = req.Amount
	n.highestBidder = req.BidderId

	// snapshot peer addresses
	peerAddrs := make([]string, 0, len(n.peerConns))
	for a := range n.peerConns {
		peerAddrs = append(peerAddrs, a)
	}
	n.mutex.Unlock()

	// build reachable connections (use grpc.NewClient if needed)
	reachable := make(map[string]*grpc.ClientConn, len(peerAddrs))
	for _, addr := range peerAddrs {
		n.mutex.Lock()
		conn := n.peerConns[addr]
		n.mutex.Unlock()

		if conn == nil {
			connTmp, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] connect %s failed (treat as down): %v", n.nodeID, addr, err)
				continue
			}
			n.mutex.Lock()
			if existing, ok := n.peerConns[addr]; !ok || existing == nil {
				n.peerConns[addr] = connTmp
				conn = connTmp
			} else {
				_ = connTmp.Close()
				conn = n.peerConns[addr]
			}
			n.mutex.Unlock()
		}
		if conn != nil {
			reachable[addr] = conn
		}
	}

	totalConfigured := len(peerAddrs) + 1
	totalReachable := len(reachable) + 1
	majority := totalReachable/2 + 1

	log.Printf("[%s] configured peers=%d reachable peers=%d (including self), need majority=%d",
		n.nodeID, totalConfigured-1, totalReachable-1, majority)

	if totalReachable == 1 {
		log.Printf("[%s] only leader reachable: committed seq=%d bidder=%s amount=%d", n.nodeID, seq, req.BidderId, req.Amount)
		return &proto.BidResponse{Status: proto.BidResponse_SUCCESS, Message: fmt.Sprintf("Bid of %d accepted", req.Amount)}, nil
	}

	acks := 1
	acked := make(map[string]bool, len(reachable))

	log.Printf("[%s] Leader replicating seq=%d bidder=%s amount=%d need %d/%d (reachable/configured %d/%d)",
		n.nodeID, seq, req.BidderId, req.Amount, majority, totalReachable, totalReachable, totalConfigured)

	// replicate sequentially, waiting for majority
	for addr, cc := range reachable {
		log.Printf("[%s] sending replicate to %s seq=%d bidder=%s amount=%d", n.nodeID, addr, seq, req.BidderId, req.Amount)
		rctx, cancel := context.WithTimeout(ctx, ReplicationTimeout)
		resp, err := proto.NewReplicationClient(cc).ReplicateBid(rctx, &proto.ReplicateBidRequest{
			BidderId:       req.BidderId,
			Amount:         req.Amount,
			Timestamp:      time.Now().UnixNano(),
			SequenceNumber: seq,
		})
		cancel()
		if err == nil && resp != nil && resp.Success {
			acks++
			acked[addr] = true
			log.Printf("[%s] ACK from %s (%d/%d) seq=%d", n.nodeID, addr, acks, totalReachable, seq)
		} else {
			log.Printf("[%s] replicate to %s failed seq=%d: %v", n.nodeID, addr, seq, err)
			// close so future will re-create
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c == cc {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
		}

		if acks >= majority {
			// replicate async to peers
			remaining := make([]string, 0, len(reachable))
			for a := range reachable {
				if !acked[a] {
					remaining = append(remaining, a)
				}
			}
			go func(addrs []string, seqLocal int64, bidder string, amount int32) {
				for _, a := range addrs {
					n.mutex.Lock()
					conn := n.peerConns[a]
					n.mutex.Unlock()
					if conn == nil {
						connTmp, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
						if err != nil {
							log.Printf("[%s] async connect %s failed: %v", n.nodeID, a, err)
							continue
						}
						n.mutex.Lock()
						if existing, ok := n.peerConns[a]; !ok || existing == nil {
							n.peerConns[a] = connTmp
							conn = connTmp
						} else {
							_ = connTmp.Close()
							conn = n.peerConns[a]
						}
						n.mutex.Unlock()
					}
					if conn == nil {
						continue
					}
					log.Printf("[%s] async replicate to %s seq=%d bidder=%s amount=%d", n.nodeID, a, seqLocal, bidder, amount)
					rctx2, cancel2 := context.WithTimeout(context.Background(), ReplicationTimeout)
					_, err2 := proto.NewReplicationClient(conn).ReplicateBid(rctx2, &proto.ReplicateBidRequest{
						BidderId:       bidder,
						Amount:         amount,
						Timestamp:      time.Now().UnixNano(),
						SequenceNumber: seqLocal,
					})
					cancel2()
					if err2 != nil {
						log.Printf("[%s] async replicate to %s failed seq=%d: %v", n.nodeID, a, seqLocal, err2)
						n.mutex.Lock()
						if c, ok := n.peerConns[a]; ok && c == conn {
							_ = c.Close()
							n.peerConns[a] = nil
						}
						n.mutex.Unlock()
					} else {
						log.Printf("[%s] async ACK from %s seq=%d", n.nodeID, a, seqLocal)
					}
				}
			}(remaining, seq, req.BidderId, req.Amount)

			log.Printf("[%s] majority reached (%d/%d) for seq=%d; committed", n.nodeID, acks, totalReachable, seq)
			return &proto.BidResponse{Status: proto.BidResponse_SUCCESS, Message: fmt.Sprintf("Bid of %d accepted", req.Amount)}, nil
		}
	}

	// majority not reached -> rollback
	n.mutex.Lock()
	if hadPrevBid {
		n.bids[req.BidderId] = prevBidValue
	} else {
		delete(n.bids, req.BidderId)
	}
	n.highestBid = prevHighest
	n.highestBidder = prevHighestBidder
	n.seq = prevSeq
	n.mutex.Unlock()

	log.Printf("[%s] majority NOT reached (%d/%d reachable) for seq=%d; rolled back", n.nodeID, acks, totalReachable, prevSeq)
	return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: "Could not replicate to majority"}, nil
}

// replicateToFollowers best-effort helper
func (n *AuctionNode) replicateToFollowers(bidder string, amount int32, ts, seq int64) {
	req := &proto.ReplicateBidRequest{BidderId: bidder, Amount: amount, Timestamp: ts, SequenceNumber: seq}
	n.mutex.Lock()
	conns := make(map[string]*grpc.ClientConn, len(n.peerConns))
	for a, c := range n.peerConns {
		conns[a] = c
	}
	n.mutex.Unlock()
	success := 1
	for a, c := range conns {
		client := proto.NewReplicationClient(c)
		ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		r, err := client.ReplicateBid(ctx, req)
		cancel()
		if err == nil && r != nil && r.Success {
			success++
		} else {
			log.Printf("[%s] replicate to %s failed: %v", n.nodeID, a, err)
		}
	}
	if success < (len(conns)+2)/2 {
		log.Printf("[%s] replicate majority not reached %d/%d", n.nodeID, success, len(conns)+1)
	}
	if success < (len(conns) + 1) {
		log.Printf("[%s] replicate to all not reached %d/%d", n.nodeID, success, len(conns)+1)
	}
}

// ReplicateBid RPC (follower-side)
func (n *AuctionNode) ReplicateBid(ctx context.Context, req *proto.ReplicateBidRequest) (*proto.ReplicateBidResponse, error) {
	n.mutex.Lock()
	n.bids[req.BidderId] = req.Amount
	if req.Amount > n.highestBid {
		n.highestBid = req.Amount
		n.highestBidder = req.BidderId
	}
	if req.SequenceNumber > n.seq {
		n.seq = req.SequenceNumber
	}
	log.Printf("[%s] follower applied replicate seq=%d bidder=%s amount=%d -> sending ACK", n.nodeID, req.SequenceNumber, req.BidderId, req.Amount)
	n.mutex.Unlock()
	return &proto.ReplicateBidResponse{Success: true}, nil
}

func (n *AuctionNode) Close() {
	n.mutex.Lock()
	for _, c := range n.peerConns {
		_ = c.Close()
	}
	n.mutex.Unlock()
}
