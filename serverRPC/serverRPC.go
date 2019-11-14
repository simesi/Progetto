package main

import (
	"Progetto/rpcInterface"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func main() {
	//Create an instance of Arith interface
	work := new(rpcInterface.Worker)

	// Register a new rpc server
	server := rpc.NewServer()
	err := server.RegisterName("Worker", work)
	if err != nil {
		log.Fatal("Format of service Worker is not correct: ", err)
	}
	// Register an HTTP handler for RPC messages on rpcPath, and a debugging handler on debugPath
	server.HandleHTTP("/", "/debug")

	// Listen for incoming messages on port 1234
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("Listen error: ", e)
	}

	// Start go's http server on socket specified by l
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal("Serve error: ", err)
	}
}
