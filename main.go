package main

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/raft"
	"os"
	"time"
)

func main() {
	fmt.Fprint(os.Stdout, "raft node starting .....\n")
	peers := []raft.Peer{{1, nil}, {2, nil}, {3, nil}, {4, nil}, {5, nil}}
	addrs := []string{":8801", ":8802", ":8803", ":8804", ":8805"}
	nodes := make([]*Node, 0)

	// nt := newRaftNetwork(1, 2, 3, 4, 5)
	// for i := 1; i <= 5; i++ {
	// 	n := StartNode(uint64(i), peers, addrs, nt.nodeNetwork(uint64(i)))
	// 	nodes = append(nodes, n)
	// }

	transports := make([]Transport, 5)
	for i := 1; i <= 5; i++ {
		transports[i-1] = newtcpTransport(uint64(i), peers, addrs)
	}
	for i := 1; i <= 5; i++ {
		n := StartNode(uint64(i), peers, addrs, transports[i-1])
		nodes = append(nodes, n)
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 10000; i++ {
		c := NewCommand()
		c.Op = OpPut
		c.K = fmt.Sprint(i)
		c.V = fmt.Sprint("v", i)
		data, e := c.Marshal()
		if e != nil {
			fmt.Fprintln(os.Stdout, "marshal command error:", e)
		}
		nodes[0].Propose(context.TODO(), data)
	}
	time.Sleep(50000 * time.Millisecond)
	for _, n := range nodes {
		if n.state.Commit != 10006 {
			fmt.Fprintf(os.Stderr, "commit = %d, want = 10006", n.state.Commit)
		}
	}
	fmt.Fprintf(os.Stdout, "commit = %d \n", nodes[0].state.Commit)
	fmt.Println("hello")
	fmt.Fprint(os.Stdout, "raft node stoped")

}
