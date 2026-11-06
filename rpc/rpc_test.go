package rpc

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/jsndz/bottle/rpc/proto"
)

func TestUnaryMessageAndResponse(t *testing.T) {
	msg, err := UnaryMessage("test.method", []byte("payload"), map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Method != "test.method" || !bytes.Equal(msg.Payload, []byte("payload")) {
		t.Fatalf("unexpected message: %+v", msg)
	}

	resp := NewUnaryResponse(msg, []byte("response"), "")
	if resp.Id != msg.Id || resp.Method != "test.method" || !bytes.Equal(resp.Payload, []byte("response")) {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestFrameReadWrite(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer l.Close()

	payload := []byte("hello frames")

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = writeFrame(conn, payload)
	}()

	clientConn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer clientConn.Close()

	buf, err := readFrame(clientConn)
	if err != nil {
		t.Fatalf("readFrame failed: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatalf("expected %s, got %s", payload, buf)
	}
}

func TestServerAndClient(t *testing.T) {
	srv := NewServer("0")
	srv.Handler.AddHandler("echo", func(ctx context.Context, msg *pb.Message) *pb.Message {
		return NewUnaryResponse(msg, msg.Payload, "")
	}, nil)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleConn(conn, srv.Handler)
		}
	}()

	pool := NewGlobalPool()
	client := NewClient(l.Addr().String(), pool)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.Call(ctx, "echo", []byte("world"), nil)
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
	if !bytes.Equal(resp.Payload, []byte("world")) {
		t.Fatalf("expected 'world', got %s", resp.Payload)
	}

	pingResp, err := client.Ping(ctx)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if pingResp.Type != pb.FrameType_HEARTBEAT {
		t.Fatalf("expected heartbeat type, got %v", pingResp.Type)
	}
}
