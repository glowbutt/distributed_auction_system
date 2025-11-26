How to run:

1) Open a terminal and run:
    go run server/server.go -id=leader -port=8080 -leader=true -peers=localhost:8081,localhost:8082
   Any port can be
2) In a separate terminal, run a new server/node:
    go run server/server.go -id=follower1 -port=8081 -leader=false -peers=localhost:8080,localhost:8082
3)
    go run server/server.go -id=follower2 -port=8082 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8081 &