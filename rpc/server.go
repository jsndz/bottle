package rpc

import (
	"context"
	"net"
	"time"

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
		var ctx context.Context
		deadline, _ := req.Headers["X-Timeout"]

		d, _ := time.Parse(time.RFC3339Nano, deadline)
		ctx, cancel := context.WithDeadline(context.Background(), d)
		defer cancel()

		var res *pb.Message
		if req.Type == pb.FrameType_HEARTBEAT {
			res = &pb.Message{
				Id:   req.Id,
				Type: pb.FrameType_HEARTBEAT,
			}

			data, err := proto.Marshal(res)
			if err != nil {
				return
			}
			_ = writeFrame(conn, data)
			continue
		} else if req.Type == pb.FrameType_STREAM_START {
			fn, _ := handler.GetStreamMethod(req.Method)
			stream := &ServerStream{
				id:   req.Id,
				conn: conn,
			}
			go fn(ctx, req, stream)
		} else if req.Type == pb.FrameType_UNARY {
			fn, ok := handler.Get(req.Method)
			if !ok {
				res = &pb.Message{
					Id:      req.Id,
					Payload: []byte("method not found: " + req.Method),
				}
			} else {
				res = fn(ctx, req)
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
