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

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
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
	HardState, ConfState, _ := c.Storage.InitialState()
	raftlog := newLog(c.Storage)
	raftlog.committed = HardState.Commit
	lastIndex := raftlog.LastIndex()
	Prs := map[uint64]*Progress{}
	if c.peers == nil {
		c.peers = ConfState.GetNodes()
	} else {
		for _, peerid := range c.peers {
			Prs[peerid] = &Progress{
				Match: 0,
				Next:  lastIndex + 1,
			}
		}
		Prs[c.ID].Match = lastIndex
	}

	return &Raft{
		id:               c.ID,
		Term:             HardState.Term,
		Vote:             HardState.Vote,
		RaftLog:          raftlog,
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Prs:              Prs,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//preIndex := r.Prs[to].Next - 1
	//preLogTerm := uint64(0)
	//if preIndex == 0 {
	//	preLogTerm = 0
	//} else {
	//	preLogTerm, _ = r.RaftLog.Term(preIndex)
	//}
	//lastIndex := r.RaftLog.LastIndex()
	//var ents []*pb.Entry
	//for i := preIndex + 1; i <= lastIndex; i++ {
	//	ents = append(ents, r.RaftLog.GetEntry(i))
	//}
	//
	//r.msgs = append(r.msgs, pb.Message{
	//	MsgType: pb.MessageType_MsgAppend,
	//	To:      to,
	//	From:    r.id,
	//	Term:    r.Term,
	//	LogTerm: preLogTerm,
	//	Index:   preIndex,
	//	Entries: ents,
	//	Commit:  r.RaftLog.committed,
	//})
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader

	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Term:    r.Term,
		Entries: []*pb.Entry{{}},
	})
}

func (r *Raft) NewElection() {
	r.Vote = r.id
	r.votes = map[uint64]bool{}
	r.votes[r.id] = true

	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	r.electionElapsed = -rand.Intn(r.electionTimeout) - 1

	for peerid := range r.Prs {
		if peerid != r.id {
			logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      peerid,
				Term:    r.Term,
				LogTerm: logterm,
				Index:   r.RaftLog.LastIndex(),
			})
		}
	}

}

func (r *Raft) Voteable(m pb.Message) bool {
	if r.Term == m.Term && r.Vote != m.From && r.Vote != None {
		return false
	}
	lastterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if m.LogTerm > lastterm || (m.LogTerm == lastterm && m.Index >= r.RaftLog.LastIndex()) {
		r.Vote = m.From
		return true
	}
	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
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

func (r *Raft) StepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.Term = m.Term
			r.Vote = None
		}
		if m.Term < r.Term {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  !r.Voteable(m),
			})
		}
	case pb.MessageType_MsgAppend:
		if m.Term < r.Term {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    m.To,
				Term:    r.Term,
				Reject:  true,
			})
		} else {
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.NewElection()
	}
}

func (r *Raft) StepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if m.Term < r.Term {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    m.To,
				Term:    r.Term,
				Reject:  true,
			})
		} else {
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			break
		} else if m.Term < r.Term {
			break
		}
		r.votes[m.From] = !m.Reject
		countfor := 0
		countagainst := 0
		for _, v := range r.votes {
			if v {
				countfor++
			} else {
				countagainst++
			}
		}

		if countfor >= len(r.Prs)/2+1 {
			r.becomeLeader()
			for k := range r.Prs {
				if k != r.id {
					r.sendHeartbeat(k)
				}
			}
		} else if countagainst >= len(r.Prs)/2+1 {
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  !r.Voteable(m),
			})
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
	case pb.MessageType_MsgHup:
		r.Term++
		r.NewElection()
	}
}

func (r *Raft) StepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgPropose:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		for k := range r.Prs {
			if k != r.id {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgHeartbeat,
					To:      k,
					From:    r.id,
					Term:    r.Term,
				})
			}
		}
		r.heartbeatElapsed = 0
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  !r.Voteable(m),
			})
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Term = max(r.Term, m.Term)
	//
	//if m.Index==0 && r.RaftLog.LastIndex()==0 {
	//	for _,ent:=range m.Entries {
	//		r.RaftLog.entries=append(r.RaftLog.entries,*ent)
	//	}
	//	r.msgs = append(r.msgs,pb.Message{
	//		MsgType: pb.MessageType_MsgAppendResponse,
	//		To: m.From,
	//		From: r.id,
	//		Term: r.Term,
	//		Index: r.RaftLog.LastIndex(),
	//		Reject: false,
	//	})
	//	if m.Commit>r.RaftLog.committed{
	//		r.RaftLog.committed=min(r.RaftLog.LastIndex(),m.Commit)
	//	}
	//}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		To:      m.From,
	})
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
