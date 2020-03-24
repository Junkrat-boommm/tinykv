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

	//lastIndex uint64
	//lastTerm uint64
	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	raft := &Raft{
		id:      c.ID,
		Term:    None,
		Vote:    None,
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower,
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

		peers: c.peers,
	}

	for _, id := range raft.peers {
		raft.Prs[id] = &Progress{Next: None, Match: None}
	}

	return raft
}

func (r *Raft) lastIndex() uint64 {
	return r.RaftLog.LastIndex()
}

func (r *Raft) lastTerm() uint64 {
	t, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	return t
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
		if nextIndex > r.RaftLog.LastIndex() || nextIndex < offset {
			log.Infof("[error] sendAppend from: %v, id: %v", r.id, to)
			return false
		}
		var entries []*pb.Entry
		for i := nextIndex; i < r.lastIndex(); i++ {
			entries = append(entries, &r.RaftLog.entries[i-offset])
		}
		//entries := r.RaftLog.getEntries(nextIndex, r.lastIndex)
		if entries == nil || len(entries) == 0 {
			return false
		}
		r.Send(pb.Message{To: to, MsgType: pb.MessageType_MsgAppend, Index: r.lastIndex(), LogTerm: r.lastTerm(), Commit: r.RaftLog.committed})
		log.Info("[sendAppend] from: %v, to: %v, entries: [%v:%v], lastterm: %v Commit: %v", r.id, to, nextIndex, r.lastIndex, r.lastTerm, r.RaftLog)
	} else {
		return false
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// If you need to send out a message just push it to `raft.Raft.msgs` and all messages the raft received will be pass to `raft.Raft.Step()`
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  r.RaftLog.committed, // To allow followers to submit logs
		Index:   r.lastIndex(),       //
		LogTerm: r.lastTerm(),
	}
	r.Send(m)
}

func (r *Raft) sendRequestVote(to uint64) {
	m := pb.Message{To: to, MsgType: pb.MessageType_MsgRequestVote, Index: r.lastIndex(), LogTerm: r.lastTerm()}
	r.Send(m)
}

// where is sendRequestVote, send when handling msg_hug

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
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed > r.randomizedElectionTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
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
	r.State = StateFollower
	r.resetTimeout()
	log.Infof("[become follower] id: %v, term: %v", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.Vote = r.id // vote for itself
	//clear the votes map
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
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
	for _, i := range r.peers {
		r.Prs[i].Match = 0
		r.Prs[i].Next = r.lastIndex() + 1
	}
	log.Infof("[become candidate] id: %v, term: %v", r.id, r.Term)
}

func (r *Raft) resetTimeout() {
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	if r.State == StateCandidate {
		r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	}
}

func (r *Raft) bcastRequestvote() {
	for _, i := range r.peers {
		r.sendRequestVote(i)
	}
}

func (r *Raft) bcastHeartbeat() {
	log.Infof("[bcastHeartbeat] id: %v, peers: %v", r.id, r.peers)
	for _, i := range r.peers {
		if i != r.id { //shouldn't send heartbeat to itself
			r.sendHeartbeat(i)
		}
	}
}

func (r *Raft) bcastAppend() {
	for _, i := range r.peers {
		r.sendAppend(i)
	}
}

// method for leader to append entries to its log
func (r *Raft) appendEntries(m pb.Message) { // TODO: determine r.Term
	if m.Entries == nil || len(m.Entries) == 0 {
		return
	}
	for i := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{EntryType: m.Entries[i].EntryType, Index: r.lastIndex() + 1, Term: r.Term, Data: m.Entries[i].Data})
		log.Infof("[leader appendEntries] id: %v", r.id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) StepLeader(m pb.Message) error {
	log.Infof("lastIndex: %v", r.lastIndex())
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		log.Infof("[error] Leader receiver a MsgHup, id: %v", r.id)
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose: //
		r.appendEntries(m)
		r.bcastAppend()
	case pb.MessageType_MsgAppend:
		r.appendEntries(m)
		r.bcastAppend()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVoteResponse: // drop
	case pb.MessageType_MsgSnapshot: //TODO
	case pb.MessageType_MsgHeartbeat: // drop
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:

	default:
		{
		}
	}
	return nil
}

func (r *Raft) StepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.bcastRequestvote()
	case pb.MessageType_MsgBeat:
		log.Infof("[error] Follower receive a MsgBeat, id: %v", r.id)
	case pb.MessageType_MsgPropose:
		// MessageType_MsgPropose' is stored in follower's mailbox(msgs) by the send method.
		r.Send(m)
	case pb.MessageType_MsgAppend:
		//log.Infof("[error] Follower receive a MsgAppend, id: %v", r.id)
		r.resetTimeout()
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: // drop
	case pb.MessageType_MsgRequestVoteResponse: //drop
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
	case pb.MessageType_MsgBeat:
		log.Infof("[error] Follower receive a MsgBeat, id: %v", r.id)
	case pb.MessageType_MsgPropose: // drop
	case pb.MessageType_MsgAppend: // why candidate can receive this message
		r.becomeFollower(m.Term, None)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: // drop
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
		//calculates how many votes it has won
	case pb.MessageType_MsgSnapshot: //TODO
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	}
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
	log.Infof("m.Term: %v, r.Term: %v", m.Term, r.Term)
	if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
			return nil
		case pb.MessageType_MsgAppend:
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Reject: true})
			return nil
		}
		//r.Send(pb.Message{MsgType: pb.MessageType_MsgTransferLeader})
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch m.MsgType {
	// everyone can receive
	case pb.MessageType_MsgRequestVote:
		//if m.Term >= r.Term {
		r.Term = m.Term // To make the term of node r grow faster
		//} else {r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})}

		if (r.Vote == None || r.Vote == r.id) && r.IsUpdated(m) {
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
			r.Vote = m.From
			r.becomeFollower(m.Term, 0)
		} else {
			r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default:
		{
		}
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

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Reject == true {
		r.votes[m.From] = true
		approve := 0
		for i := range r.peers {
			v, ok := r.votes[uint64(i)]
			if ok { // make sure this is a valid vote
				if v {
					approve++
				}
			}
			if approve > len(r.peers)/2 {
				r.becomeLeader()
			}
		}
	} else {
		r.votes[m.From] = false
		reject := 0
		for i := range r.peers {
			v, ok := r.votes[uint64(i)]
			if ok { // make sure this is a valid vote
				if v {
					reject++
				}
			}
			if reject > len(r.peers)/2 {
				r.becomeFollower(r.Term, None)
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Only one leader in the same term, so don't need to charge
	r.becomeFollower(m.Term, m.From)
	if m.Entries == nil || len(m.Entries) == 0 {
		//response.Term = r.Term
		r.Send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Reject: true})
		return
	}
	// return false if the term number of the log entry at prevLogIndex and prevLogTerm do not match
	prevLogTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || prevLogTerm != m.LogTerm {
		r.Send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Reject: true})
		return
	}
	for i := uint64(0); i < uint64(len(m.Entries)); i++ {
		if t, err := r.RaftLog.Term(m.Index + i); err != nil || t != m.Entries[i].Term {
			r.RaftLog.Delete(m.Index + i)
			// append all
			for j := i; j < uint64(len(m.Entries)); j++ {
				r.RaftLog.Append(*m.Entries[j])
			}
			break
		}
	}
	//set commitIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.lastIndex())
	}
	r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.lastIndex(), Reject: false}) // the Index help the leader update nextIndex and matchIndex
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	switch m.Reject {
	//update nextIndex, matchIndex, committed
	case true:
		r.Prs[m.From].Next, r.Prs[m.From].Match = m.Index+1, m.Index

		// update committed
		matchn := 0
		for N := r.lastIndex(); N > r.RaftLog.committed; N = N / 2 {
			for i := range r.peers {
				if r.Prs[uint64(i)].Match >= N {
					matchn++
				}
			}
			if matchn > len(r.peers)/2 {
				r.RaftLog.committed = N
			}
		}
	case false:
		// make nextIndex small
		r.Prs[m.From].Next-- // can be optimized
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	/* When 'MessageType_MsgHeartbeat' is passed to candidate and message's term is higher than candidate's,
	 * the candidate reverts back to follower and updates its committed index from the one in this heartbeat.
	 * And it sends the message to its mailbox. ??? why
	 */
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
		// don't need to set committed, i don't remember why
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
		r.Send(m)
	}
	r.Lead = m.From
	r.Send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse, Index: r.lastIndex(), Commit: r.RaftLog.committed})
}

// handleHeartbeatResponse handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Commit)
	// charge if need to send append
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
