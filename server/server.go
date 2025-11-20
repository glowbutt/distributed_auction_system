package main

import (
	"context"
	"fmt"
	"log"
	proto "main/grpc"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	logFile *os.File
	logMu   sync.Mutex
	logger  *log.Logger
)

const nodeCount = 3

var startPort = 8080

var nodes []*Node

// Contains the highest bidder and their bid.
type Data struct {
	bidID    int64
	bidVALUE int64
}
type Node struct {
	proto.UnimplementedAuctionServiceServer
	mutex sync.Mutex // Lock
	id    int64
	port  int64

	isLeader    bool
	isSync      bool
	dataCh      chan Data
	leaderCh    chan string         // Used by the synchronous server to send a reply to the Leader
	connections map[int64]chan Data // Maps follower ID to Data channel
}

func main() {
	// set up logging
	var err error
	logFile, err = os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}

	flags := log.Ldate | log.Ltime | log.Lmicroseconds
	logger = log.New(logFile, "", flags)
	defer logFile.Close()

	// initial log
	logger.Printf("Logging initialized")

	// We start by assuming node 0 is the leader and node 1 is the synchronous follower (Semi-synchronous)
	nodes := make([]*Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		port := startPort + i
		n := &Node{
			port:        int64(port),
			id:          int64(i),
			dataCh:      make(chan Data),
			connections: nil, // Only leader needs a populated channelMap
			leaderCh:    nil, // Only the synchronous follower needs to know the leader channel
		}
		nodes[i] = n
		if nodes[0].id != n.id {
			nodes[0].connections[n.id] = n.dataCh // Populate leaders connections
		}
		go n.startServer() // listener
		logEvent(n, "Started server")
	}
	nodes[0].setSynchronousFollower()

	// allow servers to properly start up
	time.Sleep(100 * time.Millisecond)

	// start connecting and run loops
	for _, n := range nodes {
		go n.run()
	}

	// block forever
	select {}
}

// all nodes constantly await data
func (n *Node) run() {
	for {
		n.updateData()
	}
}

func (n *Node) sendQuery(ctx context.Context, req *proto.ClientDetails) (*proto.Result, error) {

	// receive ID from ClientDetails
	// Use ID to return current bid from map (n.bid_data[id])

	return &proto.Result{Result: "Success"}, nil
}

// RPC function
func (n *Node) bid(ctx context.Context, req *proto.Bid) (*proto.Result, error) {
	bidderId := req.Id
	amount := req.Amount

	if amount > n.bid_VALUE {
		n.mutex.Lock()
		n.bid_VALUE = amount
		n.bid_ID = bidderId
		n.mutex.Unlock()
		return &proto.Result{Result: "Success"}, nil
	} else if amount <= n.bid_VALUE {
		return &proto.Result{Result: "Failure, bid too low"}, nil
	} else {
		return &proto.Result{Result: "Exception"}, nil
	}
	//n.sendBid(int64(id), int64(amount))
	//nodes[0].channelMap[n.id] = n.inputCh
}

func (n *Node) setSynchronousFollower() {
	// use id from leader. Increment by one. (if the id doesnt exit, move on)
	// Set the node with this ID (That is, Leader id + 1) to be the sync follower.
	// To do that, set isSync = true and channelMap equal to Leader's channel map
	// We also need to set a channel to the Leader, so we can respond, but dont know how to do this yet
	// Set the sync follower leaderCh to be equal to the Leader's leaderCh

	for i := n.id + 1; i < nodeCount; i++ {
		if nodes[i] != nil {
			follower := nodes[i]
			follower.isSync = true
			follower.leaderCh = n.leaderCh
			follower.channelMap = n.channelMap
		}
	}
}

func (n *Node) sendDataToNodes() {
	for _, c := range n.channelMap {
		c <- n.bid_data
	}
	<-n.leaderCh // awaiting response from Synchronous server
}

func (n *Node) crash() {
	// Add new node to queue so that we can always find a sync folower in SetSyncFollower
}

func (n *Node) failover() {

}

func (n *Node) updateData() {
	// Waits for the input channel to be populated with new data (the newest mapping), then populates local bid_data
	temp := <-n.inputCh
	n.bid_data = temp

	// If we are the leader, then we send our newly acquired data to everyones channels
	if n.isLeader {
		n.sendDataToNodes()
	}

	if n.isSync {
		n.leaderCh <- "Data Updated Correctly"
	}
}

// Should start server for a different address each time (8080, 8081...) one for each node
// Server only handles receive and reply
func (n *Node) startServer() {
	addr := fmt.Sprintf(":%d", n.port) // so its not hardcoded anymore
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatalf("[ERROR]: %v", err)
	}

	// Makes new GRPC server
	grpcServer := grpc.NewServer()

	// Registers the grpc server with the System struct
	proto.RegisterAuctionServiceServer(grpcServer, n)
	log.Printf("node %d listening on %s\n", n.port, addr) //this is logged in the file as well on line 73

	err = grpcServer.Serve(listener)
	if err != nil {
		logger.Fatalf("[ERROR]: %v", err)
	}
}

// ...interface means that function can accept any number of arguments of any type”
func logEvent(n *Node, format string, args ...interface{}) {
	logMu.Lock()
	defer logMu.Unlock()
	msg := fmt.Sprintf(format, args...)
	logger.Printf(
		"[NODE %d | port %d | state %d | lamport=%d], %s",
		n.id, n.port, msg,
	)
}
