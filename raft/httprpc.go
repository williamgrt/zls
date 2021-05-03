package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"sync"
)

type HttpRpcClient struct {
	mu            sync.Mutex
	clientMap     map[string]*rpc.Client
	T, D, I, W, E *log.Logger
}

type HttpRpcServer struct {
	lis           net.Listener
	local         *Peer
	T, D, I, W, E *log.Logger
}

func MakeHttpRpcClient(T, D, I, W, E *log.Logger) *HttpRpcClient {
	return &HttpRpcClient{
		mu:        sync.Mutex{},
		clientMap: make(map[string]*rpc.Client),
		T:         T,
		D:         D,
		I:         I,
		W:         W,
		E:         E,
	}
}

func MakeHttpRpcServer(local *Peer, T, D, I, W, E *log.Logger) *HttpRpcServer {
	return &HttpRpcServer{
		lis:   nil,
		local: local,
		T:     T,
		D:     D,
		I:     I,
		W:     W,
		E:     E,
	}
}

var mutex sync.Mutex
var handleTypes map[string]bool
var handleHttp bool

func (hc *HttpRpcClient) Call(p *Peer, serviceMethod string, args, reply interface{}) error {
	hc.mu.Lock()
	peer := p.String()
	client, ok := hc.clientMap[peer]
	if !ok {
		newClient, err := rpc.DialHTTP("tcp", p.Host+":"+p.Port)
		if err != nil {
			hc.W.Printf("Dial http to %s failed: %v", p.Host+":"+p.Port, err)
			hc.mu.Unlock()
			return nil
		}
		hc.D.Printf("")
		hc.clientMap[peer] = newClient
		client = newClient
	}
	hc.D.Printf("DailHttp to %s, ok.", p.Host+":"+p.Port)
	err := client.Call(serviceMethod, args, reply)
	if err != nil {
		hc.W.Printf("%s(%v) to %s failed: %v", serviceMethod, args, peer, err)
	}
	return err
}

func (hc *HttpRpcClient) Close() error {
	hc.mu.Lock()
	for _, c := range hc.clientMap {
		err := c.Close()
		if err != nil {
			hc.W.Printf("Close client %v failed", c)
		}
	}
	hc.clientMap = make(map[string]*rpc.Client)
	hc.mu.Unlock()
	return nil
}

func (hs *HttpRpcServer) Register(rcvr interface{}) error {
	mutex.Lock()
	defer mutex.Unlock()
	if handleTypes == nil {
		handleTypes = make(map[string]bool)
	}
	typeName := reflect.TypeOf(rcvr).String()
	if _, exist := handleTypes[typeName]; !exist {
		handleTypes[typeName] = true
		return rpc.Register(rcvr)
	}
	return nil
}

func (hs *HttpRpcServer) Start() error {
	mutex.Lock()
	defer mutex.Unlock()
	if !handleHttp {
		rpc.HandleHTTP()
		handleHttp = true
	}
	lis, err := net.Listen("tcp", hs.local.Host+":"+hs.local.Port)
	if err != nil {
		return err
	}
	hs.lis = lis
	hs.D.Printf("Listening on %v", hs.lis.Addr())
	go http.Serve(hs.lis, nil)
	return nil
}

func (hs *HttpRpcServer) Close() error {
	mutex.Lock()
	defer mutex.Unlock()
	if hs.lis != nil {
		hs.D.Printf("Stopping listening on %v", hs.lis.Addr())
		handleHttp = false
		err := hs.lis.Close()
		if err != nil {
			return err
		}
	}
	return nil
}