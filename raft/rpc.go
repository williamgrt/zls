package raft

type RpcServer interface {
	Register(rcvr interface{}) error
	Start() error
	Stop() error
}

type RpcClient interface {
	Call(p *Peer, serviceMethod string, args, reply interface{}) error
	Close() error
}