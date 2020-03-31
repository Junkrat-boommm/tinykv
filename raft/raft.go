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

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.

	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:      c.ID,
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower,
		votes:   make(map[uint64]bool), // reset while becoming candidate
		msgs:    make([]pb.Message, 0),
		// heartbeat interval
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	snapShot, _ := c.Storage.Snapshot()

	hardState, _, _ := c.Storage.InitialState()

	raft.setHardState(hardState)

	for _, id := range snapShot.Metadata.ConfState.Nodes {
		raft.Prs[id] = &Progress{Next: None, Match: None}
	}

	for _, id := range c.peers {
		if raft.Prs[id] == nil {
			raft.Prs[id] = &Progress{Next: None, Match: None}
		}
	}

	raft.resetTimeout() // set a initial randomizedElectionTimeout

	return raft
}

func (r *Raft) setHardState(hs pb.HardState) {
	et := pb.HardState{}
	if et.Term == hs.Term && et.Commit == hs.Term && et.Vote == hs.Vote {
		return
	}
	r.Vote = hs.Vote
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
}

func (r *Raft) lastIndex() uint64 {
	return r.RaftLog.LastIndex()
}

func (r *Raft) lastTerm() uint64 {
	t, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err == nil {
		return t
	} else {
		return 0
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// without snapshot in 2A
	if prs := r.Prs[to]; prs != nil {
		nextIndex := prs.Next
		offset := r.RaftLog.getOffset()
		if nextIndex < offset {
			panic("need to send snapshot")
			return false
		}
		if nextIndex > r.lastIndex() {
			// Used to update commits for followers
			r.Send(pb.Message{To: to, MsgType: pb.MessageType_MsgAppend, Index: nextIndex - 1, LogTerm: r.Term, Commit: r.RaftLog.committed})
			return true
		}
		var entries []*pb.Entry
		for i := nextIndex; i <= r.lastIndex(); i++ {
			entries = append(entries, &r.RaftLog.entries[i-offset])
		}
		logTerm, _ := r.RaftLog.Term(nextIndex - 1)
		// note: Index should be prevlogIndex
		r.Send(pb.Message{To: to, MsgType: pb.MessageType_MsgAppend, Index: nextIndex - 1, LogTerm: logTerm, Commit: r.RaftLog.committed, Entries: entries})
		log.Infof("[sendAppend] from: %v, to: %v, entries: %v, len: %v, data: %v, lastterm: %v Commit: %v", r.id, to, entries, len(entries), entries[0].Data, r.lastTerm(), r.RaftLog.committed)
	} else {
		return false
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		// To allow followers to submit logs
		Commit: min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.Send(m)
}

func (r *Raft) sendRequestVote(to uint64) {
	m := pb.Message{To: to, MsgType: pb.MessageType_MsgRequestVote, Index: r.lastIndex(), LogTerm: r.lastTerm()}
	r.Send(m)
}

// tick advances the internal logical clock by a single tick.
// using for charging if the node need to do something, just like a timer. It will be called every clock
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
	case StateFollower, StateCandidate:
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = None
	if r.State != StateFollower {
		r.State = StateFollower
		log.Infof("[become follower] id: %v, term: %v", r.id, r.Term)
	}

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	// Vote for itself
	r.Vote = r.id
	// Clear the votes map
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.State = StateCandidate
	// Reset election time
	r.resetTimeout()
	log.Infof("[become candidate] id: %v, term: %v", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.resetTimeout()
	r.State = StateLeader
	for i, _ := range r.Prs {
		if i == r.id {
			r.Prs[i].Match = r.lastIndex()
			r.Prs[i].Next = r.lastIndex() + 1
			continue
		}
		r.Prs[i].Match = 0
		r.Prs[i].Next = r.lastIndex() + 1
	}
	// Append a noop entry, should set data to nil
	r.appendEntries(pb.Message{Entries: []*pb.Entry{&pb.Entry{Term: r.Term, Index: r.lastIndex() + 1, Data: nil}}})
	r.bcastAppend()
	log.Infof("[become leader] id: %v, term: %v, lastindex: %v, r.prs: %v", r.id, r.Term, r.lastIndex(), r.Prs[1])
}

func (r *Raft) resetTimeout() {
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) bcastRequestvote() {
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for i, _ := range r.Prs {
		if i != r.id {
			r.sendRequestVote(i)
		}
	}
}

func (r *Raft) bcastHeartbeat() {
	for i, _ := range r.Prs {
		if i != r.id {
			r.sendHeartbeat(i)
		}
	}
}

func (r *Raft) bcastAppend() {
	for i, _ := range r.Prs {
		if i != r.id {
			r.sendAppend(i)
		}
	}
}

// method for leader to append entries to its log
func (r *Raft) appendEntries(m pb.Message) { // TODO: determine r.Term
	if m.Entries == nil || len(m.Entries) == 0 {
		return
	}
	for i := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{EntryType: m.Entries[i].EntryType, Index: r.lastIndex() + 1, Term: r.Term, Data: m.Entries[i].Data})
		log.Infof("[leader appendEntries] id: %v, lastindex: %v, lastterm: %v", r.id, r.lastIndex(), r.Term)
	}
	r.Prs[r.id].Match, r.Prs[r.id].Next = r.lastIndex(), r.lastIndex()+1
	// If it is the only peer, directly commit
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.lastIndex()
	}
}

func (r *Raft) Send(m pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
	log.Infof("[send] %v", m)
}

// Determine if the log is updated
func (r *Raft) IsUpdated(m pb.Message) bool {
	if r.RaftLog.LastTerm() < m.LogTerm || (r.RaftLog.LastTerm() == m.LogTerm && r.RaftLog.LastIndex() <= m.Index) {
		return true
	}
	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// Check Message's term
	log.Infof("[step %v] from: %v, to: %v, m.Term: %v, r.Term: %v, lastIndex: %v", m.MsgType, m.From, r.id, m.Term, r.Term, m.Index)
	if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
			return nil
		case pb.MessageType_MsgAppend:
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Reject: true})
			return nil
		}
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch m.MsgType {
	// Everyone can receive
	case pb.MessageType_MsgRequestVote:
		if (r.Vote == None || r.Vote == m.From) && r.IsUpdated(m) {
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false, Index: r.lastIndex()})
			r.Vote = m.From
			r.resetTimeout()
		} else {
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
		return nil
	default:
	}
	switch r.State {
	case StateFollower:
		r.StepFollower(m)
	case StateCandidate:
		r.StepCandidate(m)
	case StateLeader:
		r.StepLeader(m)
	}
	return nil
}

func (r *Raft) StepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // Drop
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.appendEntries(m)
		r.bcastAppend()
	case pb.MessageType_MsgAppend:
		r.appendEntries(m)
		r.bcastAppend()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVoteResponse: // Drop
	case pb.MessageType_MsgSnapshot: //TODO
	case pb.MessageType_MsgHeartbeat: // Drop
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	default:
	}
	return nil
}

func (r *Raft) StepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.bcastRequestvote()
	case pb.MessageType_MsgBeat: // Drop
	case pb.MessageType_MsgPropose:
		r.Send(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: // Drop
	case pb.MessageType_MsgRequestVoteResponse: //Drop
	case pb.MessageType_MsgSnapshot: //TODO
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) StepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.bcastRequestvote()
	case pb.MessageType_MsgBeat: // drop
	case pb.MessageType_MsgPropose: // drop
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: // drop
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgSnapshot: //TODO
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	log.Infof("[handleVoteResponse] from: %v, to: %v, reject: %v", m.From, r.id, m.Reject)
	if m.Reject == false {
		r.votes[m.From] = true
		approve := 0
		for i, _ := range r.Prs {
			v, ok := r.votes[i]
			if ok { // Make sure this is a valid vote
				if v {
					approve++
				}
			}
			if approve > len(r.Prs)/2 {
				r.becomeLeader()
				return
			}
		}
	} else {
		r.votes[m.From] = false
		reject := 0
		for i, _ := range r.Prs {
			v, ok := r.votes[i]
			if ok { // Make sure this is a valid vote
				if !v {
					reject++
				}
			}
			if reject >= (len(r.Prs)+1)/2 {
				r.becomeFollower(r.Term, None)
				// To be candidate later
				r.resetTimeout()
				return
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Only one leader in the same term, so no need to judge
	r.becomeFollower(m.Term, m.From)
	r.resetTimeout()
	// return false if the term number of the log entry at prevLogIndex and prevLogTerm do not match
	prevLogTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || prevLogTerm != m.LogTerm {
		r.Send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Reject: true})
		log.Infof("prevlogindex: %v, prevlogterm: %v, logterm: %v", m.Index, prevLogTerm, m.LogTerm)
		return
	}
	for i := uint64(0); i < uint64(len(m.Entries)); i++ {
		if t, err := r.RaftLog.Term(m.Index + i + 1); err != nil || t != m.Entries[i].Term { // Find the conflicting index
			if m.Index+i != r.lastIndex() {
				r.RaftLog.stabled = m.Index + i
				log.Infof("stabled changed")
			}
			r.RaftLog.Delete(m.Index + i + 1)
			// Append all
			for j := i; j < uint64(len(m.Entries)); j++ {
				r.RaftLog.Append(*m.Entries[j])
			}
			break
		}
	}
	//Delete for TestHandleMessageType_MsgAppend2AB
	// r.RaftLog.Delete(m.Index + uint64(len(m.Entries) + 1))
	// Set commitIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries))) // Cannot use r.lastIndex()
	}
	// The Index help the leader update nextIndex and matchIndex
	r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.lastIndex(), Reject: false})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	switch m.Reject {
	case false:
		r.Prs[m.From].Next, r.Prs[m.From].Match = m.Index+1, m.Index
		// Update committed
		for N := r.lastIndex(); N > r.RaftLog.committed; N = N - 1 {
			//Only log entries from the leader’s current term are committed by counting replicas.
			t, _ := r.RaftLog.Term(N)
			if t != r.Term {
				break
			}
			matchn := 0
			for _, v := range r.Prs {
				if v.Match >= N {
					matchn++
				}
			}
			if matchn > len(r.Prs)/2 {
				r.RaftLog.committed = N
				log.Infof("[update committed] id: %v, committed: %v", r.id, r.RaftLog.committed)
				// Add for TestLeaderCommitEntry2AB
				r.bcastAppend()
			}
		}
	case true:
		// Make nextIndex small
		r.Prs[m.From].Next-- // Can be optimized by using rejectHint
		r.sendAppend(m.From) // Add for TestLeaderSyncFollowerLog2AB
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.becomeFollower(m.Term, m.From)
	r.resetTimeout()
	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	}
	r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse, Index: r.lastIndex(), Commit: r.RaftLog.committed})
}

// handleHeartbeatResponse handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
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

func (r *Raft) HardState() pb.HardState {
	return pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}
}

func (r *Raft) SoftState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) IsEmptySnap(sp *pb.Snapshot) bool {
	return sp == nil || sp.Metadata == nil
}
