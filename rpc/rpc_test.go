package rpc

import (
	"testing"
	"time"

	pb "github.com/jsndz/bottle/rpc/proto"
)

func TestRPCCall(t *testing.T) {
	handler := NewHandler()
	handler.AddHandler("Ping", func(req *pb.Request) *pb.Response {
		return &pb.Response{
			Id:      req.Id,
			Payload: []byte("Pong: " + string(req.Payload)),
		}
	})

	port := "9090"
	go func() {
		if err := Serve(port, handler); err != nil {
			t.Logf("Serve error: %v", err)
		}
	}()

	// Give server time to listen
	time.Sleep(100 * time.Millisecond)

	req := &pb.Request{
		Id:      1,
		Method:  "Ping",
		Payload: []byte("Hello Bottle"),
	}

	resp, err := Call(port, req)
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}

	if string(resp.Payload) != "Pong: Hello Bottle" {
		t.Fatalf("unexpected payload: %s", string(resp.Payload))
	}
}
