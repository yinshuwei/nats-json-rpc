package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
	njrpc "github.com/yinshuwei/nats-json-rpc"
	"go.uber.org/zap"
)

const appName = "demo_v1"

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	natsURL := "nats://127.0.0.1:4222"
	natsConn, err := nats.Connect(natsURL,
		nats.Name(appName),
		nats.DontRandomize(),
		nats.MaxReconnects(3),
		nats.ReconnectWait(2*time.Second),
		nats.ClosedHandler(func(conn *nats.Conn) {
			conn.Close()
		}),
	)
	if err != nil {
		logger.Error("connect nats server failed", zap.Error(err))
	}
	cli := njrpc.NewClientWithConn(natsConn, natsURL, logger)

	for i := 0; i < 100; i++ {
		reply := map[string]int{}
		err = cli.Call(appName, "Arith.Add", map[string]int{"A": 1, "B": i}, &reply)
		if err != nil {
			log.Println(err)
		}

		log.Println(reply)
	}
}
