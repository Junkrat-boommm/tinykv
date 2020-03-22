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
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1].
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64 //using in configuration change

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	PendingConfIndex uint64

	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	lastIndex uint64
	lastTerm uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	raft := &Raft{
		id: c.ID,
		Term: None,
		Vote: None,
		RaftLog: newLog(c.Storage),
		Prs: make(map[uint64]*Progress),
		State: StateFollower,
		// votes records
		votes: make(map[uint64]bool), // reset while becoming candidate
		// msgs need to send
		msgs: make([]pb.Message, 0),
		// the leader id
		Lead: None,
		// heartbeat interval
		heartbeatTimeout: c.HeartbeatTick,
		// baseline of election interval
		electionTimeout: c.ElectionTick,
		// randomizedElectionTimeout is a random number between
		randomizedElectionTimeout: 0, // set while becoming candidatelue is not zero.

		leadTransferee: 0, //using in configuration change

		PendingConfIndex: None,

		electionElapsed: 0,

		heartbeatElapsed: 0,
	}

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// without snapshot in 2A
	if prs := r.Prs[to]; prs != nil {
		//matchIndex := prs.Match
		nextIndex := prs.Next
		offset := r.RaftLog.getOffset()
		if nextIndex > r.lastIndex || nextIndex < offset {return false}
		var entries []*pb.Entry
		for i := nextIndex; i < r.lastIndex; i++ {
			entries = append(entries, &r.RaftLog.entries[i])
		}
		//entries := r.RaftLog.getEntries(nextIndex, r.lastIndex)
		if entries == nil || len(entries) == 0 {return false}
		r.Send(pb.Message{To: to, MsgType: pb.MessageType_MsgAppend, Index: r.lastIndex, LogTerm: r.lastTerm, Commit: r.RaftLog.committed})
		log.Info("[sendAppend] from: %v, to: %v, entries: [%v:%v], lastterm: %v Commit: %v", r.id, to, nextIndex, r.lastIndex, r.lastTerm, r.RaftLog)
	} else {return false}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// If you need to send out a message just push it to `raft.Raft.msgs` and all messages the raft received will be pass to `raft.Raft.Step()`
	m := pb.Message{
		To: to,
		//From: r.id,
		//Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit: r.RaftLog.committed,// To allow followers to submit logs
	}
	r.Send(m)
}

// where is sendRequestVote, send when handling msg_hug

// tick advances the internal logical clock by a single tick.
// using for charging if the node need to do something, just like a timer. It will be called every clock
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed ++
		if r.heartbeatElapsed >= r.heartbeatTimeout {// need to send heartbeat
			r.heartbeatTimeout = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})// don't go over the network
		}
	case StateFollower :
		r.heartbeatElapsed ++
		//r.electionElapsed ++
		if r.heartbeatElapsed >= r.randomizedElectionTimeout {// need to re-selection
			r.heartbeatElapsed = 0
			r.electionElapsed = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		}
	case StateCandidate:
		r.electionTimeout ++
		if r.electionElapsed > r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		}
	}
}



// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.resetTimeout()
	log.Infof("[become follower] id: %v, term: %v", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	//r.votes = 0 //reset vote
	r.Vote = r.id // vote for itself
	//clear the votes map
	for i, _ := range r.votes{
		r.votes[i] = false
	}
	// TODO
	r.State = StateCandidate
	r.resetTimeout()
	log.Infof("[become candidate] id: %v, term: %v", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.resetTimeout()
	r.State = StateLeader
	log.Infof("[become candidate] id: %v, term: %v", r.id, r.Term)
}

func (r *Raft) resetTimeout() {
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	if r.State == StateCandidate {
		r.randomizedElectionTimeout = r.electionElapsed + rand.Intn(r.electionElapsed)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) StepLeader(m pb.Message) error {
	return nil
}

func (r *Raft) StepFollower(m pb.Message) error {
	return nil
}

func (r *Raft) StepCandidate(m pb.Message) error {
	return nil
}


func (r *Raft) Send(m pb.Message) {
	//if &m == nil {return}
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

// charge if the log is updated
func (r *Raft) IsUpdated(m pb.Message) bool {
	if ((r.Vote == 0) || (r.Vote == r.id)) && (r.RaftLog.LastTerm() < m.LogTerm || (r.RaftLog.LastTerm() == m.LogTerm && r.RaftLog.LastIndex() <= m.Index)) {
		return true
	}
	return false
}

// there some Message Type won't go over the network, Msghug and Msgbeat
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// check Message's term
	//if(m.Term < r.Term) {
	//
	//		return nil
	//} else if(m.Term >= r.Term) {
	//	if m.MsgType == pb.MessageType_MsgRequestVote {
	//		voteResponse := pb.Message{
	//		From:   r.id,
	//		To:     m.From,
	//		Term: r.Term,
	//	}
	//		if m.Term < r.Term { // voted false, send response
	//
	//			r.msgs = append(r.msgs, voteResponse)
	//		}
	//	}
	//}

	switch m.MsgType {
	// everyone can receive
	case pb.MessageType_MsgRequestVote:
		if m.Term >= r.Term {
			r.Term = m.Term// To make the term of node r grow faster
		} else {r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})}

		if (r.Vote == None || r.Vote == r.id) && r.IsUpdated(m){
			r.Send(pb.Message {To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
			r.Vote = m.From
			r.becomeFollower(m.Term, 0)
		} else {
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default: {}
	}
	switch r.State {
	case StateFollower:
		r.StepFollower(m)
	case StateCandidate:
		r.StepFollower(m)
	case StateLeader:
		r.StepFollower(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
