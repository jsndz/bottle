package rpc

import (
	"context"
	"net"
	"time"

	"github.com/google/uuid"
	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	ID      string
	Pool    *GlobalPool
	MaxConn uint8
	Port    string
}

func NewClient(port string, maxConn uint8, pool *GlobalPool) *Client {
	return &Client{
		ID:      uuid.NewString(),
		Pool:    pool,
		MaxConn: maxConn,
		Port:    port,
	}
}

func (c *Client) NewConnection() *Connection {
	conn, err := net.Dial("tcp", ":"+c.Port)
	if err != nil {
		return nil
	}

	return &Connection{
		ID:        uuid.NewString(),
		Conn:      &conn,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		Busy:      false,
	}
}

// Call is UNARY
func (c *Client) Call(ctx context.Context, method string, payload []byte, headers map[string]string) (*pb.Message, error) {
	connection := c.Pool.Get(c.Port)
	conn := *connection.Conn
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
	}
	req, err := UnaryMessage(method, payload, headers)
	if err != nil {
		return nil, err
	}
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	if err := writeFrame(conn, reqBytes); err != nil {
		return nil, err
	}

	respBytes, err := readFrame(conn)
	if err != nil {
		return nil, err
	}

	resp := &pb.Message{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, err
	}
	if resp.Type == pb.FrameType_UNARY {
		c.Pool.Release(c.Port, connection.ID)
	}

	return resp, nil
}

// client side it for asking data

func (c *Client) StartStream(ctx context.Context, method string, payload []byte, headers map[string]string) (<-chan *pb.Message, error) {
	connection := c.Pool.Get(c.Port)
	conn := *connection.Conn
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
	}
	req, err := StreamRequestMessage(method, payload, headers)
	if err != nil {
		return nil, err
	}
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	if err := writeFrame(conn, reqBytes); err != nil {
		return nil, err
	}
	ch := make(chan *pb.Message, 100)
	go func() {
		defer close(ch)
		defer c.Pool.Release(c.Port, connection.ID)

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
