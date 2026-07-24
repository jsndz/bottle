package rpc

import (
	"net"

	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

func Call(port string, req *pb.Request) (*pb.Response, error) {
	conn, err := net.Dial("tcp", ":"+port)
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

	resp := &pb.Response{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, err
	}

	return resp, nil
}
