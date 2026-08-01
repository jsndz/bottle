package rpc

import (
	"net"

	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	Port string
}

func NewServer(port string) *Server {
	return &Server{
		Port: port,
	}
}

func handleConn(conn net.Conn, handler *Handler) {
	defer conn.Close()

	for {
		buf, err := readFrame(conn)
		if err != nil {
			return
		}

		req := &pb.Message{}
		if err := proto.Unmarshal(buf, req); err != nil {
			return
		}

		fn, ok := handler.Get(req.Method)
		var res *pb.Message
		if !ok {
			res = &pb.Message{
				Id:      req.Id,
				Payload: []byte("method not found: " + req.Method),
			}
		} else {
			res = fn(req)
		}

		data, err := proto.Marshal(res)
		if err != nil {
			return
		}
		_ = writeFrame(conn, data)
	}
}

func (s *Server) Serve(port string, handler *Handler) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	defer lis.Close()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go handleConn(conn, handler)
	}
}
