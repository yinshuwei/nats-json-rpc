package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	njrpc "github.com/yinshuwei/nats-json-rpc"
	"go.uber.org/zap"
)

const appName = "demo_v1"

// Args Args
type Args struct {
	A, B int
}

// Reply Reply
type Reply struct {
	C int
}

// Arith Arith
type Arith struct{}

// Add Add
func (t *Arith) Add(args *Args, reply *Reply) error {
	reply.C = args.A + args.B
	log.Println(*args)
	return nil
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	natsConn, err := nats.Connect("nats://127.0.0.1:4222",
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

	server := njrpc.NewServer(logger)
	server.Register(&Arith{})
	server.ServeConn(context.Background(), natsConn, appName)
}
