How to run:

1) Open a terminal and run:
    go run server/server.go -id=leader -port=8080 -leader=true -peers=localhost:8081,localhost:8082

2) In a separate terminal, run a new server/node:
    go run server/server.go -id=follower1 -port=8081 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8082

3)
    go run server/server.go -id=follower2 -port=8082 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8081

4)
    go run client/client.go -id=alice -startport=8081


    go run client/client.go -id=bob -servers=localhost:8080,localhost:8081,localhost:8082