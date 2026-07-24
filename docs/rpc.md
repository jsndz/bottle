RPC: Remote Procedure Call

RPC is a like REST but on different principle.

The client will send a method and params then deserialize and get method run it in the server
and send the response back.

Here for serialisation and deserialisation we use protobuf 

we can do

```go 
    // pb is your .proto defination 
    // proto is protobuf lib
	req := &pb.Request{}
	err = proto.Unmarshal(buf[:n], req)
	if err != nil {
		return
	}

```