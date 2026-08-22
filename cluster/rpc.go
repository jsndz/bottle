package cluster

import pb "github.com/jsndz/bottle/rpc/proto"

type RPCResult struct {
	NodeId   string
	Response *pb.Message
	Err      error
}
