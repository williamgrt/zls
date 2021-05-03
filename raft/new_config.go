package raft

import "fmt"

// Peer represents a raft example
type Peer struct {
	Host string
	Port string
	Id   uint64
}

func (p *Peer) String() string {
	return fmt.Sprintf("%v:%v:%v", p.Host, p.Port, p.Id)
}

// Config contains settings for running raft
type Config struct {
	RpcType     string
	StorageType string
	WalDir      string
}
