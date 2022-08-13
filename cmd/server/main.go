package main

import (
	"github.com/xhantimda/commitlog/internal/server"
	"log"
)

func main() {
	localServer := server.NewHttpServer(":8080")
	log.Fatal(localServer.ListenAndServe())
}
