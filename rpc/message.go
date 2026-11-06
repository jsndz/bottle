package rpc

import (
	"github.com/google/uuid"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func newMessage(frameType pb.FrameType, method string, payload []byte, headers map[string]string) *pb.Message {
	return &pb.Message{
		Id:      uuid.New().ID(),
		Type:    frameType,
		Headers: headers,
		Method:  method,
		Payload: payload,
	}
}

func UnaryMessage(method string, payload []byte, headers map[string]string) (*pb.Message, error) {
	return newMessage(pb.FrameType_UNARY, method, payload, headers), nil
}

func StreamRequestMessage(method string, payload []byte, headers map[string]string) (*pb.Message, error) {
	return newMessage(pb.FrameType_STREAM_START, method, payload, headers), nil
}

func NewUnaryResponse(req *pb.Message, payload []byte, errStr string) *pb.Message {
	method := ""
	var id uint32
	if req != nil {
		method = req.Method
		id = req.Id
	}
	return &pb.Message{
		Id:      id,
		Type:    pb.FrameType_UNARY,
		Method:  method,
		Payload: payload,
		Error:   errStr,
	}
}

