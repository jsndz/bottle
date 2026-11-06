package rpc

import (
	"context"
	"errors"
	"math"
	"math/rand"

	"net"
	"time"

	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	Pool     *GlobalPool
	MaxRetry uint
	Addr     string
}

func NewClient(addr string, pool *GlobalPool) *Client {
	return &Client{
		Pool:     pool,
		MaxRetry: 3,
		Addr:     addr,
	}
}

func (c *Client) prepareConn(ctx context.Context, headers map[string]string) (*Connection, net.Conn, map[string]string, error) {
	connection := c.Pool.Get(c.Addr)
	if connection == nil || connection.Conn == nil || *connection.Conn == nil {
		return nil, nil, nil, errors.New("failed to acquire connection to " + c.Addr)
	}
	conn := *connection.Conn
	if headers == nil {
		headers = make(map[string]string)
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
		headers["X-Timeout"] = deadline.Format(time.RFC3339Nano)
	}
	return connection, conn, headers, nil
}

// PING
func (c *Client) Ping(ctx context.Context) (*pb.Message, error) {
	connection, conn, _, err := c.prepareConn(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer c.Pool.Release(c.Addr, connection.ID)

	pingMsg := &pb.Message{
		Type: pb.FrameType_HEARTBEAT,
	}
	reqBytes, err := proto.Marshal(pingMsg)
	if err != nil {
		return nil, err
	}

	if err := writeFrame(conn, reqBytes); err != nil {
		var retries uint = 0
		if retryErr := c.Retry(ctx, conn, reqBytes, &retries); retryErr != nil {
			return nil, retryErr
		}
	}
	respBytes, err := readFrame(conn)
	if err != nil {
		return nil, err
	}

	resp := &pb.Message{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// Call is UNARY
func (c *Client) Call(ctx context.Context, method string, payload []byte, headers map[string]string) (*pb.Message, error) {
	connection, conn, headers, err := c.prepareConn(ctx, headers)
	if err != nil {
		return nil, err
	}
	defer c.Pool.Release(c.Addr, connection.ID)

	req, _ := UnaryMessage(method, payload, headers)
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	if err := writeFrame(conn, reqBytes); err != nil {
		var retries uint = 0
		if retryErr := c.Retry(ctx, conn, reqBytes, &retries); retryErr != nil {
			return nil, retryErr
		}
	}

	respBytes, err := readFrame(conn)
	if err != nil {
		return nil, err
	}

	resp := &pb.Message{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// client side it for asking data
func (c *Client) StartStream(ctx context.Context, method string, payload []byte, headers map[string]string) (<-chan *pb.Message, error) {
	connection, conn, headers, err := c.prepareConn(ctx, headers)
	if err != nil {
		return nil, err
	}

	req, _ := StreamRequestMessage(method, payload, headers)
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		c.Pool.Release(c.Addr, connection.ID)
		return nil, err
	}

	if err := writeFrame(conn, reqBytes); err != nil {
		c.Pool.Release(c.Addr, connection.ID)
		return nil, err
	}
	ch := make(chan *pb.Message, 100)
	go func() {
		defer close(ch)
		defer c.Pool.Release(c.Addr, connection.ID)

		for {
			respBytes, err := readFrame(conn)
			if err != nil {
				return
			}
			resp := &pb.Message{}
			proto.Unmarshal(respBytes, resp)
			ch <- resp
			if resp.Type == pb.FrameType_STREAM_END {
				break
			}
		}
	}()
	return ch, nil
}

func (c *Client) Retry(ctx context.Context, conn net.Conn, payload []byte, retry *uint) error {
	if *retry >= c.MaxRetry {
		return errors.New("")
	}
	err := writeFrame(conn, payload)
	exp := time.Duration(math.Pow(2, float64(*retry))) * time.Second
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond

	t := exp + jitter
	if err != nil {
		time.Sleep(t)
		*retry++
		c.Retry(ctx, conn, payload, retry)
	}
	return err
}
