package njrpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var errMissingParams = errors.New("njrpc: request body missing params")

type serverCodec struct {
	msgs         chan *nats.Msg
	c            *nats.Conn
	params       *([]byte)
	pending      map[uint64]*nats.Msg
	mutex        sync.Mutex // protects seq, pending
	seq          uint64
	subscription *nats.Subscription
}

func newServerCodec(natsConn *nats.Conn, queue string) *serverCodec {
	msgs := make(chan *nats.Msg)
	subscription, err := natsConn.QueueSubscribe(queue, queue, func(msg *nats.Msg) {
		msgs <- msg
	})
	failOnError(err, "Failed to QueueSubscribe")

	return &serverCodec{
		msgs:         msgs,
		c:            natsConn,
		pending:      map[uint64]*nats.Msg{},
		subscription: subscription,
	}
}

func (c *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	msg := <-c.msgs

	if msg == nil {
		return errors.New("njrpc: stop reading")
	}

	r.ServiceMethod = msg.Header.Get("ServiceMethod")
	c.params = &msg.Data

	c.mutex.Lock()
	c.seq++
	r.Seq = c.seq
	c.pending[c.seq] = msg
	c.mutex.Unlock()

	return nil
}

func (c *serverCodec) ReadRequestBody(x interface{}) error {
	if x == nil {
		return nil
	}
	if c.params == nil {
		return errMissingParams
	}

	return jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(*c.params, x)
}

func (c *serverCodec) WriteResponse(r *rpc.Response, x interface{}) error {
	c.mutex.Lock()
	msg, ok := c.pending[r.Seq]
	if !ok {
		c.mutex.Unlock()
		return errors.New("invalid sequence number in response")
	}
	delete(c.pending, r.Seq)
	c.mutex.Unlock()

	m := nats.Msg{
		Subject: msg.Reply,
		Reply:   "",
		Header: nats.Header{
			"Error": []string{r.Error},
		},
	}
	if r.Error == "" {
		data, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(x)
		if err != nil {
			return fmt.Errorf("marshal response: %s", err)
		}
		m.Data = data
	}

	if err := c.c.PublishMsg(&m); err != nil {
		return fmt.Errorf("publish replay error: %s", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("publish replay ack err: %s", err)
	}

	return nil
}

func (c *serverCodec) Close() error {
	c.c.Close()
	return nil
}

type Server struct {
	s      *rpc.Server
	c      *serverCodec
	logger *zap.Logger
}

func NewServer(_logger *zap.Logger) *Server {
	if _logger == nil {
		panic("logger is nil")
	}
	return &Server{
		s:      rpc.NewServer(),
		logger: _logger,
	}
}

func (server *Server) Register(rcvr interface{}) error {
	return server.s.Register(rcvr)
}

func (server *Server) ServeConn(ctx context.Context, natsConn *nats.Conn, queue string) {
	server.c = newServerCodec(natsConn, queue)
	go server.handleSignals(ctx)
	server.s.ServeCodec(server.c)
}

func (server *Server) handleSignals(ctx context.Context) {
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(
		signalChannel,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	pid := syscall.Getpid()
	for {
		select {
		case sig := <-signalChannel:
			switch sig {
			case syscall.SIGINT:
				server.logger.Info("Received SIGINT. Shutting down...", zap.Int("pid", pid))
				server.gracefullyShutdown()
			case syscall.SIGTERM:
				server.logger.Info("Received SIGTERM. Shutting down...", zap.Int("pid", pid))
				server.gracefullyShutdown()
			}
		case <-ctx.Done():
			server.logger.Info("Background context closed. Shutting down...", zap.Int("pid", pid), zap.Error(ctx.Err()))
			server.gracefullyShutdown()
		}
	}
}

func (server *Server) gracefullyShutdown() {
	err := server.c.subscription.Unsubscribe() // 停止接受消息
	if err != nil {
		log.Fatalf("unsubscribe error: %s", err)
	}
	server.c.msgs <- nil // 停止接受消息
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
