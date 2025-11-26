#!/bin/bash

echo "========================================="
echo "  Starting Distributed Auction System"
echo "========================================="
echo ""

# Change to the script's directory
cd "$(dirname "$0")"

# Kill any previous instances
echo "Cleaning up old processes..."
kill $(lsof -t -i:8080) 2>/dev/null
kill $(lsof -t -i:8081) 2>/dev/null
kill $(lsof -t -i:8082) 2>/dev/null
pkill -f "server.go" 2>/dev/null
sleep 2

# Start Leader
echo "Starting Leader on port 8080..."
go run server/server.go -id=leader -port=8080 -leader=true -peers=localhost:8081,localhost:8082 &
LEADER_PID=$!
sleep 3

# Start Follower 1
echo "Starting Follower1 on port 8081..."
go run server/server.go -id=follower1 -port=8081 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8082 &
FOLLOWER1_PID=$!
sleep 3

# Start Follower 2
echo "Starting Follower2 on port 8082..."
go run server/server.go -id=follower2 -port=8082 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8081 &
FOLLOWER2_PID=$!
sleep 3

echo ""
echo "========================================="
echo "  All servers running!"
echo "========================================="
echo "  Leader PID:    $LEADER_PID"
echo "  Follower1 PID: $FOLLOWER1_PID"
echo "  Follower2 PID: $FOLLOWER2_PID"
echo "========================================="
echo ""
echo "Now open new terminals and run clients:"
echo ""
echo "  go run client/client.go -id=alice -servers=localhost:8080,localhost:8081,localhost:8082"
echo "  go run client/client.go -id=bob -servers=localhost:8080,localhost:8081,localhost:8082"
echo ""
echo "========================================="
echo "Press Ctrl+C to stop all servers :-)"
echo "========================================="


# Wait and handle Ctrl+C
trap "echo ''; echo 'Stopping all servers...'; kill $LEADER_PID $FOLLOWER1_PID $FOLLOWER2_PID 2>/dev/null; exit" SIGINT SIGTERM

# Keep script running
wait
