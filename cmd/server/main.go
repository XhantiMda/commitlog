package main

import (
	"log"

	"github.com/xhantimda/commitlog/internal/server"
)

func main() {
	localServer := server.NewHttpServer(":8080")
	log.Fatal(localServer.ListenAndServe())

}
