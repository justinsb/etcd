package main

import (
	"flag"
	"fmt"
	"log"

	"golang.org/x/net/websocket"
	"strconv"
)

func main() {
	streamId := flag.String("stream", "", "")
	offset := flag.Uint64("start-index", 0, "The index to start dumping")
	flag.Parse()
	if *streamId == "" {
		log.Fatal("Must provide -stream flag.")
	}
	origin := "http://127.0.0.1:4001/"
	url := "ws://127.0.0.1:4001/v2/ws/streams/" + *streamId + "/" + strconv.FormatUint(*offset, 16)
	ws, err := websocket.Dial(url, "", origin)
	if err != nil {
		log.Fatal(err)
	}

	for {
		var data []byte
		err := websocket.Message.Receive(ws, &data)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received: %s.\n", data)
	}
}