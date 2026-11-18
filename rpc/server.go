package rpc

import (
	"context"
	"net"
	"time"

	pb "github.com/jsndz/bottle/rpc/proto"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	Addr    string
	Handler *Handler
}

func NewServer(port string) *Server {
	return &Server{
		Addr:    port,
		Handler: NewHandler(),
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

		if err := dispatchMessage(conn, handler, req); err != nil {
			return
		}
	}
}

func requestContext(req *pb.Message) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if deadline, ok := req.Headers["X-Timeout"]; ok && deadline != "" {
		if d, err := time.Parse(time.RFC3339Nano, deadline); err == nil {
			return context.WithDeadline(ctx, d)
		}
	}
	return ctx, func() {}
}

func dispatchMessage(conn net.Conn, handler *Handler, req *pb.Message) error {
	ctx, cancel := requestContext(req)

	switch req.Type {
	case pb.FrameType_HEARTBEAT:
		cancel()
		res := &pb.Message{
			Id:   req.Id,
			Type: pb.FrameType_HEARTBEAT,
		}
		data, err := proto.Marshal(res)
		if err != nil {
			return err
		}
		return writeFrame(conn, data)

	case pb.FrameType_STREAM_START:
		fn, ok := handler.GetStreamMethod(req.Method)
		if !ok {
			cancel()
			return nil
		}
		stream := &ServerStream{
			id:   req.Id,
			conn: conn,
		}
		go func() {
			defer cancel()
			fn(ctx, req, stream)
		}()
		return nil

	case pb.FrameType_UNARY:
		defer cancel()
		var res *pb.Message
		fn, ok := handler.Get(req.Method)
		if !ok {
			res = &pb.Message{
				Id:      req.Id,
				Type:    pb.FrameType_UNARY,
				Method:  req.Method,
				Payload: []byte("method not found: " + req.Method),
				Error:   "method not found: " + req.Method,
			}
		} else {
			res = fn(ctx, req)
		}

		data, err := proto.Marshal(res)
		if err != nil {
			return err
		}
		return writeFrame(conn, data)

	default:
		cancel()
		return nil
	}
}

func (s *Server) Serve(port string) error {
	addr := port
	if addr == "" {
		addr = s.Addr
	}
	lis, err := net.Listen("tcp", ":"+addr)
	if err != nil {
		return err
	}
	return s.ServeListener(lis)
}

func (s *Server) ServeListener(lis net.Listener) error {
	defer lis.Close()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go handleConn(conn, s.Handler)
	}
}
