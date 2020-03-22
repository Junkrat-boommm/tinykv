// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  truncated.....first.....applied....committed....stabled.....last
//  --------|     |------------------------------------------------|
//                                  log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are stabled to storage
	stabled uint64 //use for initialize a new node

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//lastIndex uint64
	//lastTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//snapshot, _ := storage.Snapshot()
	newlog := &RaftLog{storage:storage}

	// get LastIndex in latest Snapshot + 1
	firstIndex, _ := storage.FirstIndex()// TODO: handlet the situation that fisrt log is dummy
	newlog.committed = firstIndex - 1
	newlog.applied = newlog.committed

	// get LastIndex in storage
	lastIndex, _ := storage.LastIndex()
	newlog.stabled = lastIndex

	entries, _ := storage.Entries(firstIndex, lastIndex)
	newlog.entries = entries

	newlog.pendingSnapshot = nil //handle in 2C

	return newlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) getOffset() uint64 {
	if len(l.entries) < 0 {return 0}
	return l.entries[0].Index
}

func (l *RaftLog) getEntries(start, end uint64) []pb.Entry {
	offset := l.getOffset()
	if start >= offset && start <= end && end + 1 - offset <= (uint64)(len(l.entries)) {
		return l.entries[start - offset: end - offset + 1 ]
	} else {
		return nil
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	offset := l.getOffset()
	if l.stabled + 1 -offset >= (uint64)(len(l.entries)) {
		return nil
	} else {
		return l.entries[l.stabled - offset + 1 :]
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	offset := l.getOffset()
	if l.applied >= l.committed || l.committed + 1 -offset > (uint64)(len(l.entries))  || l.applied < offset {
		return nil
	} else {
		return l.entries[l.applied - offset:l.committed - offset + 1]
	}
	//return nil
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len := len(l.entries); len != 0 {
		return l.entries[len - 1].Index
	}
	return 0
}

func (l *RaftLog) LastTerm() uint64 {
	if len := len(l.entries); len != 0 {
		return l.entries[len - 1].Term
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	if i < offset || i - offset + 1 > uint64(len(l.entries)) {
		return 0, errors.New("invalid index!")
	} else {
		return l.entries[i -offset].Term, nil
	}
	//return 0, nil
}
