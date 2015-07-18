package main

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	pb "github.com/coreos/etcd/raft/raftpb"
	"os"
	"time"
)

const DirPath = "/home/sunya/repl/src/demo/data/"

type Node struct {
	raft.Node
	storage *raft.MemoryStorage
	state   raftpb.HardState
	t       Transport
	f       *os.File
}

func StartNode(id uint64, peers []raft.Peer, addrs []string, t Transport) *Node {
	st := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         st,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}
	rn := raft.StartNode(c, peers)
	n := &Node{
		Node:    rn,
		storage: st,
	}
	n.t = t
	path := fmt.Sprint(DirPath, id)
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if e != nil {
		fmt.Fprintln(os.Stdout, "open  the  kv file error:", e)
	}
	n.f = f
	go n.t.listen()
	go n.start()
	return n
}

func (n *Node) start() {
	tk := time.Tick(5 * time.Millisecond)
	for {
		select {
		case <-tk:
			n.Tick()
		case rd := <-n.Ready():
			if !raft.IsEmptyHardState(rd.HardState) {
				n.state = rd.HardState
				n.storage.SetHardState(n.state)
			}
			n.storage.Append(rd.Entries)
			n.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				n.storage.ApplySnapshot(rd.Snapshot)
			}
			time.Sleep(time.Millisecond)
			for _, entry := range rd.CommittedEntries {
				n.process(entry)
				// if entry.Type == raftpb.EntryConfChange {
				// }
				// 	var cc raftpb.ConfChange
				// 	cc.Unmarshal(entry.Data)
				// 	n.ApplyConfChange(cc)
			}
			n.Advance()
		case m := <-n.receive():
			n.Step(context.TODO(), m)
		}
	}
}

func (n *Node) send(msgs []pb.Message) error {
	for _, msg := range msgs {
		for e := n.t.send(msg); e != nil; {
			fmt.Fprint(os.Stdout, "send msg error:", e)
			e = n.t.send(msg)
		}
	}
	return nil
}

func (n *Node) receive() chan pb.Message {
	return n.t.receive()
}

func (n *Node) process(en pb.Entry) {
	if en.Type == pb.EntryNormal {
		c := new(Command)
		e := c.Unmarshal(en.Data)
		if e != nil {
			fmt.Fprintln(os.Stdout, "unmarshal the command err:", e)
		}
		switch c.Op {
		case OpPut:
			fmt.Fprintln(n.f, c.K, "->", c.V)
		default:
		}
	}
}
