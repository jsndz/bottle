package rpc

import (
	"net"

	"github.com/google/uuid"
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
		var res *pb.Message

		if req.Type == pb.FrameType_STREAM_START {
			fn, ok := handler.GetStreamMethod(req.Method)
			stream := &ServerStream{
				id:   uuid.New().ID(),
				conn: conn,
			}
			if !ok {
				res = &pb.Message{
					Id:      req.Id,
					Payload: []byte("method not found: " + req.Method),
				}
			}
			go fn(req, stream)
		} else {
			fn, ok := handler.Get(req.Method)
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
