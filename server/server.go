// File: auctionnode_fixed.go
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
)

// AuctionNode is the server node for the auction system.
type AuctionNode struct {
	proto.UnimplementedAuctionServer
	proto.UnimplementedReplicationServer

	mutex           sync.Mutex
	nodeID          string
	selfAddr        string   // canonical address for this node (host:port)
	configuredPeers []string // configured peer addresses
	isLeader        bool
	port            string
	leaderAddress   string
	peerConns       map[string]*grpc.ClientConn
	bids            map[string]int32
	highestBid      int32
	highestBidder   string
	auctionEnd      time.Time
	seq             int64
	lastElection    time.Time

	// election state
	term     int64  // current term
	votedFor string // candidate this node voted for in current term
}

func main() {
	id := flag.String("id", "node1", "node id")
	port := flag.String("port", "8080", "listening port")
	addr := flag.String("addr", "", "address this node listens on (host:port). If empty defaults to localhost:<port>")
	leader := flag.Bool("leader", false, "start as leader")
	leaderAddr := flag.String("leader-addr", "", "leader address (host:port) if known")
	peers := flag.String("peers", "", "comma separated list of peer addresses (host:port)")
	flag.Parse()

	if *addr == "" {
		*addr = fmt.Sprintf("localhost:%s", *port)
	}

	pl := []string{}
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" && p != *addr {
				pl = append(pl, p)
			}
		}
	}

	n := &AuctionNode{
		nodeID:          *id,
		isLeader:        *leader,
		port:            *port,
		leaderAddress:   *leaderAddr,
		selfAddr:        *addr,
		configuredPeers: pl,
		peerConns:       map[string]*grpc.ClientConn{},
		bids:            map[string]int32{},
		auctionEnd:      time.Now().Add(AuctionDuration),
		term:            0,
		votedFor:        "",
	}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterAuctionServer(s, n)
	proto.RegisterReplicationServer(s, n)

	// Ensure peerConns has entries for all configured peers (nil if not connected yet)
	go func() {
		time.Sleep(1 * time.Second)
		n.connectToPeers(n.configuredPeers)
	}()

	log.Printf("[%s] start addr=%s port=%s leader=%v peers=%v", n.nodeID, n.selfAddr, n.port, n.isLeader, n.configuredPeers)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// dial attempts to establish a grpc connection with a short timeout.
func dial(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), QuickTimeout)
	defer cancel()
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

// connectToPeers: best-effort initial connects; ensures configured peer keys exist in peerConns
func (n *AuctionNode) connectToPeers(peers []string) {
	// Ensure keys for configured peers exist
	n.mutex.Lock()
	for _, a := range peers {
		if a == "" || a == n.selfAddr {
			continue
		}
		if _, ok := n.peerConns[a]; !ok {
			n.peerConns[a] = nil // placeholder to indicate configured peer
		}
	}
	n.mutex.Unlock()

	for _, a := range peers {
		if a == "" || a == n.selfAddr {
			continue
		}

		// async connect
		go func(addr string) {
			conn, err := dial(addr)
			if err != nil {
				log.Printf("[%s] initial connect to %s failed: %v (keeping placeholder)", n.nodeID, addr, err)
				// keep placeholder; don't delete so we know it's a configured peer
				return
			}
			n.mutex.Lock()
			// close any existing before replacing
			if prev, ok := n.peerConns[addr]; ok && prev != nil {
				_ = prev.Close()
			}
			n.peerConns[addr] = conn
			n.mutex.Unlock()
			log.Printf("[%s] connected %s", n.nodeID, addr)
		}(a)
	}
}

// Bid RPC (client-facing)
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

// forwardBidToLeader forwards to the node in n.leaderAddress.
// If leader missing/unreachable it triggers an election and retries until deadline.
func (n *AuctionNode) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	deadline := time.Now().Add(3 * time.Second)
	for {
		n.mutex.Lock()
		leader := n.leaderAddress
		self := n.selfAddr
		conn := n.peerConns[leader]
		isLeader := n.isLeader
		n.mutex.Unlock()

		// Defensive: if leader points to ourselves but we are not marked leader, trigger an election.
		// If leader points to ourselves and we're marked leader, process locally.
		if leader == self {
			if isLeader {
				// we're actually the leader — process locally
				return n.processBid(ctx, req)
			}
			// leader advertise equals self but we aren't marked leader: try to recover by starting election
			log.Printf("[%s] forwardBidToLeader: leader points to self but isLeader=false -> trigger election", n.nodeID)
			go n.triggerElectionIfNeeded(leader)
			// small backoff to allow election to progress
			if time.Now().After(deadline) {
				return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader unreachable"}, nil
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if leader == "" {
			// no leader known -> try to elect
			go n.triggerElectionIfNeeded("")
			if time.Now().After(deadline) {
				return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader unreachable"}, nil
			}
			time.Sleep(150 * time.Millisecond)
			continue
		}

		// Try to create connection if missing or validate existing conn
		if conn == nil {
			connTmp, err := dial(leader)
			if err != nil {
				// leader seems down; trigger election and retry
				log.Printf("[%s] forward: connect to leader %s failed: %v", n.nodeID, leader, err)
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
		log.Printf("[%s] forward: Bid RPC to leader %s failed: %v", n.nodeID, leader, err)
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
	currLeader := n.leaderAddress
	last := n.lastElection
	n.mutex.Unlock()

	shouldStart := (currLeader == "" || currLeader == failed) && time.Since(last) >= electionCooldown
	if shouldStart {
		n.mutex.Lock()
		n.lastElection = time.Now()
		n.mutex.Unlock()
		go n.startElection()
	} else {
		log.Printf("[%s] triggerElectionIfNeeded: not starting election (currLeader=%q failed=%q lastElection=%v)", n.nodeID, currLeader, failed, last)
	}
}

// Result RPC (client-facing)
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
			conn, err = dial(leader)
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

// startElection implements an election and becomes leader only if majority votes.
// Uses configuredPeers as the membership and verifies reachability with dial.
func (n *AuctionNode) startElection() {
	// Snapshot state
	n.mutex.Lock()
	n.term++
	term := n.term
	selfAddr := n.selfAddr
	n.votedFor = selfAddr
	n.lastElection = time.Now()

	peers := make([]string, 0, len(n.configuredPeers))
	for _, a := range n.configuredPeers {
		if a == "" || a == selfAddr {
			continue
		}
		peers = append(peers, a)
	}
	mySeq := n.seq
	n.mutex.Unlock()

	// Determine which peers are actually reachable right now by attempting connections
	reachableConns := make(map[string]*grpc.ClientConn)
	for _, addr := range peers {
		connTmp, err := dial(addr)
		if err != nil {
			log.Printf("[%s] startElection: connect %s failed (treat as down): %v", n.nodeID, addr, err)
			// mark as nil in peerConns (keep key)
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c != nil {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			if _, ok := n.peerConns[addr]; !ok {
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
			continue
		}
		n.mutex.Lock()
		if existing, ok := n.peerConns[addr]; ok && existing != nil {
			_ = existing.Close()
		}
		n.peerConns[addr] = connTmp
		reachableConns[addr] = connTmp
		n.mutex.Unlock()
	}

	totalConfigured := len(peers) + 1
	totalReachable := len(reachableConns) + 1 // include self
	majority := totalConfigured/2 + 1

	// If only this node is reachable (none of the configured peers answered dial), auto-elect.
	if len(reachableConns) == 0 {
		n.mutex.Lock()
		prev := n.leaderAddress
		n.leaderAddress = selfAddr
		n.isLeader = true
		n.votedFor = ""
		n.mutex.Unlock()
		log.Printf("[%s] only one reachable node -> auto leader %s (configured_peers=%d) prevLeader=%s", n.nodeID, selfAddr, totalConfigured-1, prev)
		// best-effort notify configured peers
		go n.notifyPeersOfLeader(selfAddr, reachableConns)
		return
	}

	log.Printf("[%s] starting election term=%d configured_peers=%d reachable_peers=%d need_majority=%d",
		n.nodeID, term, totalConfigured-1, totalReachable-1, majority)

	votes := 1 // vote for self

	// Request votes from reachable peers only
	for addr, conn := range reachableConns {
		rctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		resp, err := proto.NewReplicationClient(conn).RequestVote(rctx, &proto.RequestVoteRequest{
			Term:        term,
			CandidateId: selfAddr,
			LastSeq:     mySeq,
		})
		cancel()

		if err != nil {
			log.Printf("[%s] RequestVote RPC to %s failed: %v", n.nodeID, addr, err)
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c == conn {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
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
			log.Printf("[%s] vote from %s (votes=%d/%d)", n.nodeID, addr, votes, totalConfigured)
			if votes >= majority {
				break
			}
		}
	}

	if votes < majority {
		log.Printf("[%s] election lost term=%d votes=%d/%d (reachable/configured %d/%d)", n.nodeID, term, votes, totalConfigured, len(reachableConns), totalConfigured-1)
		return
	}

	// won election: become leader
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = selfAddr
	n.isLeader = true
	n.votedFor = "" // clear votedFor once leader
	n.mutex.Unlock()

	log.Printf("[%s] WON election term=%d votes=%d/%d -> leader=%s (prev=%s)", n.nodeID, term, votes, totalConfigured, selfAddr, prev)

	// notifying reachable peers
	for addr, conn := range reachableConns {
		rctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		_, err := proto.NewReplicationClient(conn).MakeNewLeader(rctx, &proto.MakeNewLeaderRequest{Address: selfAddr})
		cancel()
		if err != nil {
			log.Printf("[%s] notify %s MakeNewLeader RPC failed: %v", n.nodeID, addr, err)
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c == conn {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
			continue
		}
		log.Printf("[%s] told %s -> leader=%s", n.nodeID, addr, selfAddr)
	}
}

// notifyPeersOfLeader notifies configured peers (best-effort) about leader change.
// reachableConns may contain already-open connections; we'll try others via dial.
func (n *AuctionNode) notifyPeersOfLeader(newLeader string, reachableConns map[string]*grpc.ClientConn) {
	n.mutex.Lock()
	peers := append([]string{}, n.configuredPeers...)
	n.mutex.Unlock()

	for _, addr := range peers {
		if addr == "" || addr == n.selfAddr {
			continue
		}
		conn := reachableConns[addr]
		if conn == nil {
			var err error
			conn, err = dial(addr)
			if err != nil {
				log.Printf("[%s] notifyPeersOfLeader: dial %s failed: %v", n.nodeID, addr, err)
				continue
			}
			// store connection if configured
			n.mutex.Lock()
			if _, ok := n.peerConns[addr]; ok {
				if existing := n.peerConns[addr]; existing != nil {
					_ = existing.Close()
				}
				n.peerConns[addr] = conn
			}
			n.mutex.Unlock()
		}
		rctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
		_, err := proto.NewReplicationClient(conn).MakeNewLeader(rctx, &proto.MakeNewLeaderRequest{Address: newLeader})
		cancel()
		if err != nil {
			log.Printf("[%s] notifyPeersOfLeader: MakeNewLeader to %s failed: %v", n.nodeID, addr, err)
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c == conn {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
			continue
		}
		log.Printf("[%s] notifyPeersOfLeader: told %s -> leader=%s", n.nodeID, addr, newLeader)
	}
}

// MakeNewLeader RPC: follower updates leader info
func (n *AuctionNode) MakeNewLeader(ctx context.Context, req *proto.MakeNewLeaderRequest) (*proto.MakeNewLeaderResponse, error) {
	if req == nil || req.Address == "" {
		return &proto.MakeNewLeaderResponse{Success: false, Message: "empty"}, nil
	}
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = req.Address
	n.isLeader = (n.leaderAddress == n.selfAddr)
	n.votedFor = ""
	n.mutex.Unlock()
	log.Printf("[%s] received MakeNewLeader -> leader=%s (prev=%s) isLeader=%v", n.nodeID, req.Address, prev, n.isLeader)
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

// processBid replicates synchronously to a majority before confirming to client
// Uses fresh dials to validate reachability so a dead follower won't be counted.
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

	// snapshot configured peers (not peerConns keys)
	peerAddrs := make([]string, 0, len(n.configuredPeers))
	for _, a := range n.configuredPeers {
		if a == "" || a == n.selfAddr {
			continue
		}
		peerAddrs = append(peerAddrs, a)
	}
	n.mutex.Unlock()

	// Attempt to establish fresh connections to configured peers (with QuickTimeout).
	reachable := make(map[string]*grpc.ClientConn, len(peerAddrs))
	for _, addr := range peerAddrs {
		connTmp, err := dial(addr)
		if err != nil {
			log.Printf("[%s] processBid: dial %s failed (treat as down): %v", n.nodeID, addr, err)
			// ensure stored conn is cleared
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c != nil {
				_ = c.Close()
				n.peerConns[addr] = nil
			} else if _, ok := n.peerConns[addr]; !ok {
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
			continue
		}

		// store/replace connection for this peer
		n.mutex.Lock()
		if existing, ok := n.peerConns[addr]; ok && existing != nil {
			_ = existing.Close()
		}
		n.peerConns[addr] = connTmp
		n.mutex.Unlock()

		reachable[addr] = connTmp
	}

	totalConfigured := len(peerAddrs) + 1
	majority := totalConfigured/2 + 1

	log.Printf("[%s] configured peers=%d reachable peers=%d (including self), need majority=%d",
		n.nodeID, totalConfigured-1, len(reachable), majority)

	// If no follower is actually reachable (no successful dials), commit locally immediately.
	if len(reachable) == 0 {
		log.Printf("[%s] only leader reachable (no successful dials): committed seq=%d bidder=%s amount=%d", n.nodeID, seq, req.BidderId, req.Amount)
		return &proto.BidResponse{Status: proto.BidResponse_SUCCESS, Message: fmt.Sprintf("Bid of %d accepted", req.Amount)}, nil
	}

	acks := 1
	acked := make(map[string]bool, len(reachable))

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
			log.Printf("[%s] ACK from %s (%d/%d) seq=%d", n.nodeID, addr, acks, totalConfigured, seq)
		} else {
			log.Printf("[%s] replicate to %s failed seq=%d: %v", n.nodeID, addr, seq, err)
			// close so future will re-dial
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c == cc {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
		}

		if acks >= majority {
			// replicate async to peers that haven't ACKed yet
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
						connTmp, err := dial(a)
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

			log.Printf("[%s] majority reached (%d/%d) for seq=%d; committed", n.nodeID, acks, totalConfigured, seq)
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

	log.Printf("[%s] majority NOT reached (acks=%d required=%d) for seq=%d; rolled back", n.nodeID, acks, majority, prevSeq)
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
		if c == nil {
			// attempt dial
			cc, err := dial(a)
			if err != nil {
				log.Printf("[%s] replicateToFollowers: dial %s failed: %v", n.nodeID, a, err)
				continue
			}
			// store it
			n.mutex.Lock()
			if _, ok := n.peerConns[a]; ok && n.peerConns[a] == nil {
				n.peerConns[a] = cc
			} else {
				_ = cc.Close()
			}
			n.mutex.Unlock()
			c = n.peerConns[a]
			if c == nil {
				continue
			}
		}
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
	// Apply log entry idempotently
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
