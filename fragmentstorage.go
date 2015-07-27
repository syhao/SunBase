package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	pb "repl/raft/raftpb"
	"sync"
)

const (
	SegmentSize = 4096 //4k
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var (
	ErrCompacted         = errors.New("requested index is unavailable due to compaction")
	ErrSnapOutOfDate     = errors.New("requested index is older than the existing snapshot")
	ErrEntryLength       = errors.New("get the entry length error")
	ErrEntryContentShort = errors.New("get the entry payload error,content short for expect")
)

type FragmentStorage struct {
	fents      *os.File //[]Entry saved per 4k  offset
	fhardState *os.File //return pb.HardState
	fsnapshot  *os.File //snapshot
	sync.Mutex
}

func NewFragmentStorage(path string, id uint64) (*FragmentStorage, error) {
	fs := new(FragmentStorage)
	var err error
	path = fmt.Sprintf("%sraft_%d/", path, id)
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}
	var names = []string{"entrys", "hardstate", "snapshot"}
	var fps = []**os.File{&fs.fents, &fs.fhardState, &fs.fsnapshot}
	for i, name := range names {
		*fps[i], err = os.OpenFile(path+name, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return fs, err
		}
	}
	fs.PutEntry(pb.Entry{})
	return fs, nil
}

func (fs *FragmentStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	hs := pb.HardState{}
	cs := pb.ConfState{}
	return hs, cs, nil
}

func (fs *FragmentStorage) LastIndex() (uint64, error) {
	fs.Lock()
	defer fs.Unlock()
	ret, err := fs.fents.Seek(0, 2)
	if err != nil {
		return 0, err
	}
	return uint64(ret / int64(SegmentSize)), nil
}

func (fs *FragmentStorage) FirstIndex() (uint64, error) {
	fs.Lock()
	defer fs.Unlock()
	return 1, nil
}

func (fs *FragmentStorage) getHardState() (pb.HardState, error) {
	r := bufio.NewReader(fs.fhardState)
	hs := pb.HardState{}
	if data, e := r.ReadSlice('\n'); e != nil {
		return hs, e
	} else {
		data = data[:len(data)-1]
		return hs, hs.Unmarshal(data)
	}
	return hs, nil
}

func (fs *FragmentStorage) SetHardState(st pb.HardState) error {
	fs.fhardState.Seek(0, 0)
	if data, e := st.Marshal(); e != nil {
		return e
	} else {
		_, e = fmt.Fprintln(fs.fhardState, data)
		return e
	}
	return nil
}

func (fs *FragmentStorage) Entry(i uint64) (pb.Entry, error) {
	fs.fents.Seek(int64(i*SegmentSize), 0)
	lengthByte := make([]byte, 4)
	entry := pb.Entry{}
	n, err := fs.fents.Read(lengthByte)
	if err != nil || n != 4 {
		if err != nil {
			return entry, err
		}
		return entry, ErrEntryLength
	}
	len := binary.BigEndian.Uint32(lengthByte)
	payload := make([]byte, int(len))
	n, err = fs.fents.Read(payload)
	if err != nil || n != int(len) {
		if err != nil {
			return entry, err
		}
		return entry, ErrEntryContentShort
	}
	return entry, entry.Unmarshal(payload)
}

func (fs *FragmentStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	fs.Lock()
	defer fs.Unlock()
	var entrys []pb.Entry
	for i := lo; i < hi; i++ {
		if en, err := fs.Entry(i); err != nil {
			return nil, err
		} else {
			entrys = append(entrys, en)
		}
	}
	return entrys, nil
}

// Term implements the Storage interface.
func (fs *FragmentStorage) Term(i uint64) (uint64, error) {
	fs.Lock()
	defer fs.Unlock()
	if en, err := fs.Entry(i); err != nil {
		return 0, err
	} else {
		return en.Term, nil
	}
	return 0, nil
}

func (fs *FragmentStorage) Append(entries []pb.Entry) error {
	fs.Lock()
	defer fs.Unlock()
	if len(entries) == 0 {
		return nil
	}
	first := 1
	last := entries[0].Index + uint64(len(entries)) - 1
	// shortcut if there is no new entry.
	if last < uint64(first) {
		return nil
	}
	offset := entries[0].Index - 0
	size, err := fs.fents.Seek(0, 2)
	if err != nil {
		return err
	}
	entrysSize := size/(SegmentSize) + 1
	///fmt.Println(entrysSize, offset)
	switch {
	case uint64(entrysSize) >= offset:
		currIndex := uint64(0)
		for _, en := range entries {
			currIndex = en.Index
			if err := fs.PutEntry(en); err != nil {
				return err
			}
		}
		currIndex += 1
		size = int64(currIndex*SegmentSize) - 1
		if err != nil {
			return err
		}
		return fs.fents.Truncate(size)
	default:
		return fmt.Errorf("missing log entry")
	}
	return nil
}

func (fs *FragmentStorage) PutEntry(e pb.Entry) error {
	data, err := EncodingEntry(e)
	if err != nil {
		return err
	}
	_, err = fs.fents.WriteAt(data, int64(e.Index*SegmentSize))
	return err
}

func (fs *FragmentStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	fs.Lock()
	defer fs.Unlock()
	snapshot := pb.Snapshot{}
	preSnapshot, err := fs.Snapshot()
	if err != nil {
		return snapshot, err
	}
	if i <= preSnapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}
	lastIndex, err := fs.LastIndex()
	if err != nil {
		return snapshot, err
	}
	if i > lastIndex {
		fmt.Errorf("snapshot %d is out of bound lastindex(%d)", i, lastIndex)
	}
	snapshot.Metadata.Index = i
	if en, err := fs.Entry(i); err != nil {
		return snapshot, err
	} else {
		snapshot.Metadata.Term = en.Term
	}
	if cs != nil {
		snapshot.Metadata.ConfState = *cs
	}
	snapshot.Data = data
	return snapshot, nil

}

func (fs *FragmentStorage) Snapshot() (pb.Snapshot, error) {
	snapshot := pb.Snapshot{}
	r := bufio.NewReader(fs.fsnapshot)
	data, err := r.ReadSlice('\n')
	if err != nil {
		return snapshot, err
	}
	data = data[:len(data)-1]
	return snapshot, snapshot.Unmarshal(data)
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (fs *FragmentStorage) ApplySnapshot(snap pb.Snapshot) error {
	fs.Lock()
	defer fs.Unlock()
	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	fs.fsnapshot.Seek(0, 0)
	_, err = fmt.Fprintln(fs.fsnapshot, data)
	if err != nil {
		return err
	}
	entries := []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return fs.Append(entries)
}

func EncodingEntry(e pb.Entry) ([]byte, error) {
	data, err := e.Marshal()
	if err != nil {
		return nil, err
	}
	size := len(data)
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, uint32(size))
	ret = append(ret, data...)
	return ret, nil
}
