How to run:

1) Open a terminal and run:
    go run server/server.go -id=leader -port=8080 -leader=true -peers=localhost:8081,localhost:8082

2) In a separate terminal, run a new server/node:
    go run server/server.go -id=follower1 -port=8081 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8082

3) Repeat if you want a third server:
    go run server/server.go -id=follower2 -port=8082 -leader=false -leader-addr=localhost:8080 -peers=localhost:8080,localhost:8081

4) Now open new terminals and run clients:

    go run client/client.go -id=alice -startport=8080
    go run client/client.go -id=bob -startport=8080

5) Start bidding away by writing an integer amount. It must be higher than the previous amount.
   Check status of auction by typing result! :-)

According to our tests, the program should be able to tolerate any of the following scenarios:
 - crashing the leader with 2 followers
 - crashing the leader with 1 follower
 - crashing 1/2 follower with leader
 - crashing 2/2 followers with leader
 - crashing 1/1 follower with leader
