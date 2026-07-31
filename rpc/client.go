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

func (c *Client) Call(ctx context.Context, port string, req *pb.Message) (*pb.Message, error) {
	connection := c.Pool.Get(port)
	conn := *connection.Conn
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
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
	if resp.Type == pb.FrameType_STREAM_END || resp.Type == pb.FrameType_UNARY {
		c.Pool.Release(port, connection.ID)
	}
	return resp, nil
}
