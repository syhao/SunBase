package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
	"io"
	"net"
	"os"
)

var ErrShortPacket = errors.New("the packet have not enough  length")

type Transport interface {
	send(msg pb.Message) error
	stop()
	listen() error
	receive() chan pb.Message
}

type tcpTransport struct {
	addr  string
	peerC map[uint64]net.Conn
	reciv chan pb.Message
}

type Packet struct {
	Size uint32
	Data []byte
}

func GenPacket(data []byte) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b[:4], uint32(len(data)))
	b = append(b, data...)
	return b
}

func ReadPacket(conn net.Conn) (*Packet, error) {
	p := &Packet{}
	size := make([]byte, 4)
	n, err := conn.Read(size)
	if err != nil || n != 4 {
		fmt.Fprintln(os.Stdout, "read socket head len:", n, "nend:4", err)
		return nil, err
	}
	p.Size = binary.BigEndian.Uint32(size)
	data := make([]byte, int(p.Size))
	offset := 0
	for {
		n, err = conn.Read(data[offset:])
		if err != nil && err != io.EOF {
			fmt.Fprintln(os.Stdout, "have an error", err)
		}
		offset += n
		if offset == int(p.Size) {
			break
		}
	}
	p.Data = data
	return p, nil
}

func newtcpTransport(id uint64, peers []raft.Peer, addrs []string) Transport {
	t := &tcpTransport{
		reciv: make(chan pb.Message, 1024),
	}
	t.peerC = make(map[uint64]net.Conn)
	for i, peer := range peers {
		if peer.ID != id {
			go t.initPeerClient(peer.ID, addrs[i])
		} else {
			t.addr = addrs[i]
		}
	}
	return t
}

func (t *tcpTransport) initPeerClient(id uint64, addr string) {
	var (
		conn net.Conn
		err  error
	)
	for conn, err = net.Dial("tcp", addr); err != nil; {
		conn, err = net.Dial("tcp", addr)
	}
	fmt.Println("init client id", id, conn.RemoteAddr())
	t.peerC[id] = conn
}

func (t *tcpTransport) listen() error {
	l, e := net.Listen("tcp", t.addr)
	if e != nil {
		fmt.Fprint(os.Stdout, "listen err:", e)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stdout, "accept connetion error:%s\n", err.Error())
		}
		go t.handleConnection(conn)
	}

}

func (t *tcpTransport) handleConnection(c net.Conn) {
	for {
		p, e := ReadPacket(c)
		if e != nil {
			fmt.Fprintf(os.Stdout, "read from connection err:%s\n", e.Error())
		}
		m := pb.Message{}

		if e := m.Unmarshal(p.Data); e != nil {
			fmt.Fprint(os.Stdout, "unmarshal protobuf   Message data  error:", e)
		}
		select {
		case t.reciv <- m:
			fmt.Fprintln(os.Stdout, "receive from:", m.From, "to:", m.To)
		default:
		}

	}
}

func (t *tcpTransport) receive() chan pb.Message {
	return t.reciv
}

func (t *tcpTransport) stop() {
	t = nil
	os.Exit(0)
}

func (t *tcpTransport) send(msg pb.Message) error {
	data, err := msg.Marshal()
	if err != nil {
		fmt.Fprint(os.Stdout, "marshal message err:", err)
		return err
	}
	fmt.Println(msg)
	p := GenPacket(data)
	fmt.Println(len(p))
	n, err := t.peerC[msg.To].Write(p)
	if err != nil {
		fmt.Fprintf(os.Stdout, "socket write data err:%s\n", err.Error())
		return err
	}
	if n != 4+len(data) {
		fmt.Fprintln(os.Stdout, "write data short", n, 4+len(data))
		return ErrShortPacket
	}
	fmt.Fprint(os.Stdout, msg.From, "--->", msg.To, "\n")
	return nil
}
