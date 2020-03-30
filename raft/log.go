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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//

//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	nl := &RaftLog{storage: storage}

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	log.Infof("len: %v, ents: %v", len(entries), entries)

	nl.entries = entries
	nl.committed = firstIndex - 1
	nl.applied = firstIndex - 1
	nl.stabled = lastIndex

	if snapShot, err := storage.Snapshot(); err == nil {
		nl.pendingSnapshot = &snapShot
	}
	log.Infof("newlog: %v", nl)
	return nl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) getOffset() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[0].Index
}

func (l *RaftLog) getEntries(start, end uint64) []pb.Entry {
	offset := l.getOffset()
	if start >= offset && start <= end && end+1-offset <= (uint64)(len(l.entries)) {
		return l.entries[start-offset : end-offset+1]
	} else {
		return nil
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	offset := l.getOffset()
	log.Infof("stabled: %v, offset: %v, len: %v", l.stabled, offset, len(l.entries))
	if l.stabled+1-offset > (uint64)(len(l.entries)) {
		return nil
	} else {
		return l.entries[l.stabled-offset+1:]
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	offset := l.getOffset()
	log.Infof("offset: %v, applied: %v, committed: %v, len: %v", offset, l.applied, l.committed, len(l.entries))
	if l.applied >= l.committed || l.committed+1-offset > (uint64)(len(l.entries)) || l.applied < offset-1 {
		return nil
	} else {
		return l.entries[l.applied-offset+1 : l.committed-offset+1]
	}
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len := len(l.entries); len != 0 {
		return l.entries[len-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return 0
}

func (l *RaftLog) LastTerm() uint64 {
	if len := len(l.entries); len != 0 {
		return l.entries[len-1].Term
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Term
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	offset := l.getOffset()
	if i < offset || i-offset+1 > uint64(len(l.entries)) {
		return 0, errors.New("invalid index!")
	} else {
		return l.entries[i-offset].Term, nil
	}
}

func (l *RaftLog) Delete(i uint64) error {
	offset := l.getOffset()
	if i < offset || i-offset > uint64(len(l.entries)) {
		log.Infof("offset = %v, i= %v", offset, i)
		return nil
	}
	l.entries = l.entries[:i-offset]
	return nil
}

func (l *RaftLog) Append(en pb.Entry) {
	l.entries = append(l.entries, en)
}
