package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	d "github.com/beel-ww/dfs/definitions"
)

func main() {
	// to do
	s := &d.Server{
		Clients:     make(map[string]d.ClientInfo),
		GlobalFiles: make(map[string]d.FileInfo),
	}
	err := rpc.Register(s)
	if err != nil {
		panic(err)
	}
	rpc.HandleHTTP()

	const localIP = "127.0.0.1"

	listener, err := net.Listen("tcp", net.JoinHostPort(localIP, "8080"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("DFS Server listening on %s\n", localIP)
	go http.Serve(listener, nil)
	fmt.Println("Waiting for RPC requests.")

	go func() {
		for {
			time.Sleep(4 * time.Second)
			s.DisconnectClient()
		}
	}()

	select {}
}
