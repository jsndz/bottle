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

Kuberenetes idea is reuse as much resources as possible
connection pool:

Connection pool is a set of already-established connections through which client can connect and get the data.
Seen in postgres.
when do you create a connection in connection pool?
It can be eager connection or lazy
what happens a client does not want to use connection pool?
it is basically using a single tcp connection for multiple uses.



Here Connections are associated with the client 
client decides max number of connection and based on that 
conn pool will be created.
If the connection pool is empty || all existing connection are busy then the connection will be created upto max
if not client need to wait
THere is a global connection pool for each client with each server M:N