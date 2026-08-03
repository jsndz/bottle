package rpc

import (
	"github.com/google/uuid"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func UnaryMessage(method string, payload []byte, headers map[string]string) (*pb.Message, error) {
	message := pb.Message{
		Id:      uuid.New().ID(),
		Method:  method,
		Type:    pb.FrameType_UNARY,
		Headers: headers,
		Payload: payload,
	}
	return &message, nil
}

func StreamRequestMessage(method string, payload []byte, headers map[string]string) (*pb.Message, error) {
	message := pb.Message{
		Id:      uuid.New().ID(),
		Type:    pb.FrameType_STREAM_START,
		Headers: headers,
		Method:  method,
		Payload: payload,
	}

	return &message, nil
}
