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
		if !*leader && n.leaderAddress != "" {
			n.syncStateFromLeader()
		}
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
		if c, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil {
			n.mutex.Lock()
			n.peerConns[a] = c
			n.mutex.Unlock()
			log.Printf("[%s] connected %s", n.nodeID, a)
		}
	}
}

func (n *AuctionNode) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if time.Now().After(n.auctionEnd) {
		return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: "auction ended"}, nil
	}
	if !n.isLeader {
		return n.forwardBidToLeader(ctx, req)
	}
	return n.processBid(req)
}

func (n *AuctionNode) forwardBidToLeader(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	n.mutex.Unlock()
	defer n.mutex.Lock()
	deadline := time.Now().Add(3 * time.Second)
	for {
		n.mutex.Lock()
		leader := n.leaderAddress
		conn := n.peerConns[leader]
		n.mutex.Unlock()
		if leader == "" {
			n.triggerElectionIfNeeded("")
		} else {
			if conn == nil {
				var err error
				_, dc := context.WithTimeout(ctx, QuickTimeout)
				conn, err = grpc.NewClient(leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
				dc()
				if err != nil {
					go n.triggerElectionIfNeeded(leader)
					conn = nil
				}
			}
			if conn != nil {
				client := proto.NewAuctionClient(conn)
				rctx, rc := context.WithTimeout(ctx, ReplicationTimeout)
				resp, err := client.Bid(rctx, req)
				rc()
				if err == nil {
					return resp, nil
				}
				go n.triggerElectionIfNeeded(leader)
			}
		}
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

func (n *AuctionNode) makeNewLeader(old string) {
	n.mutex.Lock()
	if n.leaderAddress != old {
		n.mutex.Unlock()
		return
	}
	peers := make([]string, 0, len(n.peerConns))
	for a := range n.peerConns {
		if a != old {
			peers = append(peers, a)
		}
	}
	n.mutex.Unlock()
	newLeader := ""
	for _, a := range peers {
		if c, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil && c != nil {
			_ = c.Close()
			newLeader = a
			break
		}
	}
	if newLeader == "" {
		newLeader = fmt.Sprintf("%s:%s", hostFallback, n.port)
	}
	n.Bury(old)
	n.mutex.Lock()
	prev := n.leaderAddress
	n.leaderAddress = newLeader
	n.isLeader = (newLeader == fmt.Sprintf("%s:%s", hostFallback, n.port))
	n.mutex.Unlock()
	for _, a := range peers {
		n.mutex.Lock()
		c := n.peerConns[a]
		n.mutex.Unlock()
		if c == nil {
			continue
		}
		go func(cc *grpc.ClientConn, addr string) {
			client := proto.NewReplicationClient(cc)
			ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
			defer cancel()
			_, _ = client.MakeNewLeader(ctx, &proto.MakeNewLeaderRequest{Address: newLeader})
			log.Printf("[%s] told %s -> leader=%s", n.nodeID, addr, newLeader)
		}(c, a)
	}
	log.Printf("[%s] leader %s -> %s", n.nodeID, prev, newLeader)
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

func (n *AuctionNode) processBid(req *proto.BidRequest) (*proto.BidResponse, error) {
	if prev, ok := n.bids[req.BidderId]; ok && req.Amount <= prev {
		return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: fmt.Sprintf("must be higher than %d", prev)}, nil
	}
	if req.Amount <= n.highestBid {
		return &proto.BidResponse{Status: proto.BidResponse_FAIL, Message: fmt.Sprintf("must be higher than %d", n.highestBid)}, nil
	}
	n.seq++
	n.bids[req.BidderId] = req.Amount
	n.highestBid = req.Amount
	n.highestBidder = req.BidderId
	ts := time.Now().UnixNano()
	go n.replicateToFollowers(req.BidderId, req.Amount, ts, n.seq)
	return &proto.BidResponse{Status: proto.BidResponse_SUCCESS, Message: fmt.Sprintf("Bid of %d accepted", req.Amount)}, nil
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

func (n *AuctionNode) ReplicateBid(ctx context.Context, req *proto.ReplicateBidRequest) (*proto.ReplicateBidResponse, error) {
	n.mutex.Lock()
	n.bids[req.BidderId] = req.Amount
	if req.Amount > n.highestBid {
		n.highestBid = req.Amount
		n.highestBidder = req.BidderId
	}
	n.seq = req.SequenceNumber
	n.mutex.Unlock()
	return &proto.ReplicateBidResponse{Success: true}, nil
}

func (n *AuctionNode) GetState(ctx context.Context, req *proto.GetStateRequest) (*proto.GetStateResponse, error) {
	n.mutex.Lock()
	bs := make([]*proto.BidEntry, 0, len(n.bids))
	for id, v := range n.bids {
		bs = append(bs, &proto.BidEntry{BidderId: id, Amount: v})
	}
	sn, ae := n.seq, n.auctionEnd.UnixNano()
	n.mutex.Unlock()
	return &proto.GetStateResponse{Bids: bs, AuctionEndTime: ae, SequenceNumber: sn}, nil
}

func (n *AuctionNode) syncStateFromLeader() {
	if n.isLeader || n.leaderAddress == "" {
		return
	}
	conn, err := grpc.NewClient(n.leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()
	client := proto.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), ReplicationTimeout)
	defer cancel()
	if st, err := client.GetState(ctx, &proto.GetStateRequest{RequesterId: n.nodeID}); err == nil && st != nil {
		n.mutex.Lock()
		for _, b := range st.Bids {
			n.bids[b.BidderId] = b.Amount
			if b.Amount > n.highestBid {
				n.highestBid = b.Amount
				n.highestBidder = b.BidderId
			}
		}
		n.seq = st.SequenceNumber
		n.auctionEnd = time.Unix(0, st.AuctionEndTime)
		n.mutex.Unlock()
	}
}

func (n *AuctionNode) Close() {
	n.mutex.Lock()
	for _, c := range n.peerConns {
		_ = c.Close()
	}
	n.mutex.Unlock()
}
