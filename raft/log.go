package raft

type LogEntry struct {
	Term int
	Data interface{}
}
