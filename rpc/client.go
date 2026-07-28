package rpc

import (
	"context"
	"net"

	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

func Call(ctx context.Context, port string, req *pb.Message) (*pb.Message, error) {
	conn, err := net.Dial("tcp", ":"+port)
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
	}
	if err != nil {
		return nil, err
	}
	defer conn.Close()

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

	return resp, nil
}
