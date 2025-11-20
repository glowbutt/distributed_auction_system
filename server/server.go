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

type Node struct {
	proto.UnimplementedAuctionServiceServer
	mutex      sync.Mutex // lock for lamport stuff
	id         int
	port       int
	inputCh    chan map[int]int         // Each server has an input channel, for receiving the newest mapping of bids
	bid_data   map[int]int              // Contains the bid data
	channelMap map[int]chan map[int]int // Leader uses this map to send to every follower channel.
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

	// We always start with node 0 is the leader
	nodes := make([]*Node, nodeCount)
	for i := 0; i <= nodeCount; i++ {
		port := startPort + i
		n := &Node{
			port:       port,
			id:         i,
			inputCh:    make(chan map[int]int),
			bid_data:   make(map[int]int),
			channelMap: nil, // Only leader needs a populated channelMap
		}
		nodes[i] = n
		nodes[0].channelMap[n.id] = n.inputCh // Populate leaders channelMap
		go n.startServer()                    // listener
		logEvent(n, "Started listening on its port")
	}

	// allow servers and connections to be made before running
	time.Sleep(100 * time.Millisecond)

	// start connecting and run loops
	for _, n := range nodes {
		go n.run()
	}

	// block forever
	select {}
}

func (n *Node) run() {
	// POSSIBLY ISSUE: Connecting before all servers are started, can be tested with logging
	for i := startPort; i < startPort+nodeCount; i++ {
		if n.port == i {
			continue
		}
	}
}

// RPC function
func (n *Node) sendBid() {
	// how do we get the ID and BID?
	n.bid_data[ID] = BID
}

func (n *Node) sendDataToNodes() {
	for _, c := range n.channelMap {
		c <- n.bid_data
	}
}

func (n *Node) updateData() {
	// Waits for the input channel to be populated with new data (the newest mapping), then populates local bid_data
	temp := <-n.inputCh
	n.bid_data = temp
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
