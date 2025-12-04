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

	n := &AuctionNode{nodeID: *id, isLeader: *leader, port: *port, leaderAddress: *leaderAddr, peerConns: map[string]*grpc.ClientConn{}, bids: map[string]int32{}, auctionEnd: time.Now().Add(AuctionDuration)}
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

func (n *AuctionNode) connectToPeers(peers []string) {
	for _, a := range peers {
		if a == "" {
			continue
		}
		n.mutex.Lock()
		// ensure an entry exists for this address (may be nil until connected)
		if _, ok := n.peerConns[a]; !ok {
			n.peerConns[a] = nil
		}
		n.mutex.Unlock()

		// try connecting asynchronously so startup doesn't block
		go func(addr string) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] initial connect to %s failed: %v", n.nodeID, addr, err)
				return
			}
			n.mutex.Lock()
			// store the connection
			n.peerConns[addr] = conn
			n.mutex.Unlock()
			log.Printf("[%s] connected %s", n.nodeID, addr)
		}(a)
	}
}

// Replace existing Bid
func (n *AuctionNode) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	n.mutex.Lock()
	if time.Now().After(n.auctionEnd) {
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

// Replace existing forwardBidToLeader
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
			dctx, dcancel := context.WithTimeout(ctx, QuickTimeout)
			connTmp, err := grpc.DialContext(dctx, leader, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			dcancel()
			if err != nil {
				go n.triggerElectionIfNeeded(leader)
				if time.Now().After(deadline) {
					return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader unreachable"}, nil
				}
				time.Sleep(150 * time.Millisecond)
				continue
			}
			n.mutex.Lock()
			// store connection for reuse
			n.peerConns[leader] = connTmp
			n.mutex.Unlock()
			conn = connTmp
		}

		client := proto.NewAuctionClient(conn)
		rctx, rc := context.WithTimeout(ctx, ReplicationTimeout)
		resp, err := client.Bid(rctx, req)
		rc()
		if err == nil {
			return resp, nil
		}
		go n.triggerElectionIfNeeded(leader)

		if time.Now().After(deadline) {
			return &proto.BidResponse{Status: proto.BidResponse_EXCEPTION, Message: "leader unreachable"}, nil
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (n *AuctionNode) triggerElectionIfNeeded(failed string) {
	n.mutex.Lock()
	if n.leaderAddress == failed && time.Since(n.lastElection) >= electionCooldown {
		n.lastElection = time.Now()
		n.mutex.Unlock()
		go n.makeNewLeader(failed)
	} else {
		n.mutex.Unlock()
	}
}

// returns the current auction state or winner
func (n *AuctionNode) Result(ctx context.Context, req *proto.ResultRequest) (*proto.ResultResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	log.Printf("[%s] Result query from %s", n.nodeID, req.BidderId)

	auctionOver := time.Now().After(n.auctionEnd)

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
			Message:     fmt.Sprintf("Auction ended.  Winner: %s with bid of %d", n.highestBidder, n.highestBid),
		}, nil
	}

	if n.highestBidder == "" {
		return &proto.ResultResponse{
			AuctionOver: false,
			HighestBid:  0,
			Message:     "Auction ongoing.  No bids yet",
		}, nil
	}

	return &proto.ResultResponse{
		AuctionOver: false,
		Winner:      n.highestBidder,
		HighestBid:  n.highestBid,
		Message:     fmt.Sprintf("Auction ongoing. Highest bid: %d by %s", n.highestBid, n.highestBidder),
	}, nil
}

func (n *AuctionNode) makeNewLeader(old string) {
	n.mutex.Lock()
	if n.leaderAddress != old {
		n.mutex.Unlock()
		return
	}
	n.lastElection = time.Now()
	peers := make([]string, 0, len(n.peerConns))
	for a := range n.peerConns {
		if a != old {
			peers = append(peers, a)
		}
	}
	selfAddr := fmt.Sprintf("%s:%s", hostFallback, n.port)
	n.mutex.Unlock()

	candidates := make([]string, 0, len(peers)+1)
	candidates = append(candidates, selfAddr)
	for _, a := range peers {
		dctx, dcancel := context.WithTimeout(context.Background(), QuickTimeout)
		conn, err := grpc.DialContext(dctx, a, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		dcancel()
		if err != nil {
			continue
		}
		_ = conn.Close()
		candidates = append(candidates, a)
	}

	if len(candidates) == 0 {
		n.mutex.Lock()
		prev := n.leaderAddress
		n.leaderAddress = selfAddr
		n.isLeader = true
		n.mutex.Unlock()
		log.Printf("[%s] leader %s -> %s (no candidates found, self elected)", n.nodeID, prev, selfAddr)
		return
	}

	chosen := candidates[0]
	for _, c := range candidates[1:] {
		if c < chosen {
			chosen = c
		}
	}

	n.Bury(old)
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = chosen
	n.isLeader = (chosen == selfAddr)
	n.mutex.Unlock()

	for _, a := range peers {
		n.mutex.Lock()
		c := n.peerConns[a]
		n.mutex.Unlock()
		if c == nil {
			connTmp, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			n.mutex.Lock()
			if existing, ok := n.peerConns[a]; !ok || existing == nil {
				n.peerConns[a] = connTmp
				c = connTmp
			} else {
				_ = connTmp.Close()
				c = n.peerConns[a]
			}
			n.mutex.Unlock()
		}
		if c == nil {
			continue
		}
		go func(cc *grpc.ClientConn, addr string) {
			client := proto.NewReplicationClient(cc)
			ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
			defer cancel()
			_, _ = client.MakeNewLeader(ctx, &proto.MakeNewLeaderRequest{Address: chosen})
			log.Printf("[%s] told %s -> leader=%s", n.nodeID, addr, chosen)
		}(c, a)
	}
	log.Printf("[%s] leader %s -> %s", n.nodeID, prev, chosen)
}

func (n *AuctionNode) MakeNewLeader(ctx context.Context, req *proto.MakeNewLeaderRequest) (*proto.MakeNewLeaderResponse, error) {
	if req == nil || req.Address == "" {
		return &proto.MakeNewLeaderResponse{Success: false, Message: "empty"}, nil
	}
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = req.Address
	n.isLeader = (n.leaderAddress == fmt.Sprintf("%s:%s", hostFallback, n.port))
	if conn, ok := n.peerConns[prev]; ok && prev != "" && prev != req.Address {
		_ = conn.Close()
		delete(n.peerConns, prev)
	}
	n.mutex.Unlock()
	return &proto.MakeNewLeaderResponse{Success: true, Message: "ok"}, nil
}

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

// Replace existing processBid (note new signature: takes ctx)
func (n *AuctionNode) processBid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	// validate & apply tentatively under lock
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

	// snapshot peer addresses (not connections) under lock, then unlock for RPCs
	peerAddrs := make([]string, 0, len(n.peerConns))
	for a := range n.peerConns {
		peerAddrs = append(peerAddrs, a)
	}
	n.mutex.Unlock()

	// build reachable connections (dial if needed). reachable map only contains peers we can actually talk to now.
	reachable := make(map[string]*grpc.ClientConn, len(peerAddrs))
	for _, addr := range peerAddrs {
		n.mutex.Lock()
		conn := n.peerConns[addr]
		n.mutex.Unlock()

		if conn == nil {
			dctx, dcancel := context.WithTimeout(ctx, QuickTimeout)
			connTmp, err := grpc.DialContext(dctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			dcancel()
			if err != nil {
				log.Printf("[%s] dial %s failed (treat as down): %v", n.nodeID, addr, err)
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

	// compute quorum based on reachable nodes only
	totalConfigured := len(peerAddrs) + 1
	totalReachable := len(reachable) + 1
	majority := totalReachable/2 + 1

	log.Printf("[%s] configured peers=%d reachable peers=%d (including self), need majority=%d",
		n.nodeID, totalConfigured-1, totalReachable-1, majority)

	if totalReachable == 1 {
		log.Printf("[%s] only leader reachable: committed seq=%d bidder=%s amount=%d",
			n.nodeID, seq, req.BidderId, req.Amount)
		return &proto.BidResponse{Status: proto.BidResponse_SUCCESS, Message: fmt.Sprintf("Bid of %d accepted", req.Amount)}, nil
	}

	acks := 1
	log.Printf("[%s] Leader replicating seq=%d bidder=%s amount=%d need %d/%d (reachable/configured %d/%d)",
		n.nodeID, seq, req.BidderId, req.Amount, majority, totalReachable, totalReachable, totalConfigured)

	acked := make(map[string]bool, len(reachable))

	// replicate to each reachable peer (sequentially)
	for addr, cc := range reachable {
		log.Printf("[%s] sending replicate to %s seq=%d bidder=%s amount=%d", n.nodeID, addr, seq, req.BidderId, req.Amount)
		rctx, rc := context.WithTimeout(ctx, ReplicationTimeout)
		resp, err := proto.NewReplicationClient(cc).ReplicateBid(rctx, &proto.ReplicateBidRequest{
			BidderId:       req.BidderId,
			Amount:         req.Amount,
			Timestamp:      time.Now().UnixNano(),
			SequenceNumber: seq,
		})
		rc()
		if err == nil && resp != nil && resp.Success {
			acks++
			acked[addr] = true
			log.Printf("[%s] ACK from %s (%d/%d) seq=%d", n.nodeID, addr, acks, totalReachable, seq)
		} else {
			log.Printf("[%s] replicate to %s failed seq=%d: %v", n.nodeID, addr, seq, err)
			// mark connection nil so future attempts will re-dial
			n.mutex.Lock()
			if c, ok := n.peerConns[addr]; ok && c == cc {
				_ = c.Close()
				n.peerConns[addr] = nil
			}
			n.mutex.Unlock()
		}

		if acks >= majority {
			// prepare list of remaining peers we haven't ACKed yet
			remaining := make([]string, 0, len(reachable))
			for a := range reachable {
				if !acked[a] {
					remaining = append(remaining, a)
				}
			}

			// asynchronously replicate to remaining peers (don't block client)
			go func(addrs []string, seqLocal int64, bidder string, amount int32) {
				for _, a := range addrs {
					// attempt dial/store if needed
					n.mutex.Lock()
					conn := n.peerConns[a]
					n.mutex.Unlock()
					if conn == nil {
						dctx, dcancel := context.WithTimeout(context.Background(), QuickTimeout)
						connTmp, err := grpc.DialContext(dctx, a, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
						dcancel()
						if err != nil {
							log.Printf("[%s] async dial %s failed: %v", n.nodeID, a, err)
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
					log.Printf("[%s] async sending replicate to %s seq=%d bidder=%s amount=%d", n.nodeID, a, seqLocal, bidder, amount)
					rctx2, rc2 := context.WithTimeout(context.Background(), ReplicationTimeout)
					resp2, err2 := proto.NewReplicationClient(conn).ReplicateBid(rctx2, &proto.ReplicateBidRequest{
						BidderId:       bidder,
						Amount:         amount,
						Timestamp:      time.Now().UnixNano(),
						SequenceNumber: seqLocal,
					})
					rc2()
					if err2 == nil && resp2 != nil && resp2.Success {
						log.Printf("[%s] async ACK from %s seq=%d", n.nodeID, a, seqLocal)
					} else {
						log.Printf("[%s] async replicate to %s failed seq=%d: %v", n.nodeID, a, seqLocal, err2)
						n.mutex.Lock()
						if c, ok := n.peerConns[a]; ok && c == conn {
							_ = c.Close()
							n.peerConns[a] = nil
						}
						n.mutex.Unlock()
					}
				}
			}(remaining, seq, req.BidderId, req.Amount)

			log.Printf("[%s] quorum reached (%d/%d) for seq=%d; committed", n.nodeID, acks, totalReachable, seq)
			return &proto.BidResponse{Status: proto.BidResponse_SUCCESS, Message: fmt.Sprintf("Bid of %d accepted", req.Amount)}, nil
		}
	}

	// quorum not reached -> rollback
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

	log.Printf("[%s] quorum NOT reached (%d/%d reachable) for seq=%d; rolled back", n.nodeID, acks, totalReachable, prevSeq)
	return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: "Could not replicate to majority"}, nil
}

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
}

// Replace existing ReplicateBid (follower-side)
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
