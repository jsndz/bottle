package rpc

import (
	"net"

	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

type ServerStream struct {
	id   uint32
	conn net.Conn
}

func (s *ServerStream) Send(payload []byte) error {
	msg := &pb.Message{Id: s.id, Type: pb.FrameType_STREAM_DATA, Payload: payload}
	bytes, _ := proto.Marshal(msg)
	return writeFrame(s.conn, bytes)
}

func (s *ServerStream) Finish() error {
	msg := &pb.Message{Id: s.id, Type: pb.FrameType_STREAM_END}
	bytes, _ := proto.Marshal(msg)
	return writeFrame(s.conn, bytes)
}
