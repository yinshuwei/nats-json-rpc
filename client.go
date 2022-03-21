package njrpc

import (
	"errors"
	"net/rpc"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// vars
var (
	ErrShutdown = rpc.ErrShutdown
)

// Client nat Client
type Client struct {
	url     string
	conn    *nats.Conn
	logger  *zap.Logger
	Timeout int64
}

// NewClientWithConn NewClientWithConn
func NewClientWithConn(conn *nats.Conn, url string, _logger *zap.Logger) *Client {
	if _logger == nil {
		panic("logger is nil")
	}
	return &Client{
		url:     url,
		conn:    conn,
		Timeout: 20,
		logger:  _logger,
	}
}

// JSONCall Call
func (client *Client) JSONCall(queue, serviceMethod string, args *[]byte, reply *[]byte) error {
	msg := &nats.Msg{
		Subject: queue,
		Data:    *args,
		Header: nats.Header{
			"ServiceMethod": []string{serviceMethod},
		},
		Sub: &nats.Subscription{
			Queue: queue,
		},
	}
	returnMsg, err := client.conn.RequestMsg(msg, time.Duration(client.Timeout)*time.Second)
	if err != nil {
		return err
	}
	remoteErr := returnMsg.Header.Get("Error")
	if remoteErr != "" {
		return errors.New(remoteErr)
	}
	*reply = returnMsg.Data
	return nil
}

// Call Call
func (client *Client) Call(subject, method string, args, receiver interface{}) error {
	startTime := time.Now()
	var argsbytes = []byte{}
	var replyBytes = []byte{}
	var err error
	defer func() {
		replyStr := string(replyBytes)
		if len(replyStr) > 1024 {
			replyStr = replyStr[:1024]
		}
		client.logger.Info("nats client call:",
			zap.String("cost", time.Since(startTime).String()),
			zap.String("queue", subject),
			zap.String("method", method),
			zap.ByteString("args", argsbytes),
			zap.String("reply", replyStr),
		)
	}()
	if client == nil {
		return errors.New("[NATS Call] client is nil")
	}
	argsbytes, err = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(args)
	if err != nil {
		return errors.New("[NATS Call] args Marshal error: " + err.Error())
	}

	err = client.JSONCall(subject, method, &argsbytes, &replyBytes)
	if err != nil {
		return errors.New("[NATS Call] remote error: " + err.Error())
	}

	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(replyBytes, receiver)
	if err != nil {
		return errors.New("[NATS Call] reply Unmarshal error: " + err.Error())
	}
	return nil
}
