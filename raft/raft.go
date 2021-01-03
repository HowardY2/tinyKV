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
	"sort"
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

	randomElectionTimeout int
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
	raftLog := newLog(c.Storage)
	lastIndex := raftLog.LastIndex()
	firstIndex, _ := raftLog.storage.FirstIndex()
	Prs := map[uint64]*Progress{}
	if c.peers == nil {
		c.peers = ConfState.Nodes
	}
	if c.peers != nil {
		for _, peerId := range c.peers {
			if peerId == c.ID {
				Prs[peerId] = &Progress{
					Next:  lastIndex + 1,
					Match: lastIndex,
				}
			} else {
				Prs[peerId] = &Progress{
					Next:  lastIndex + 1,
					Match: firstIndex - 1,
				}
			}
		}
	}
	raftLog.committed = HardState.GetCommit()
	return &Raft{
		id:                    c.ID,
		Term:                  HardState.Term,
		Vote:                  HardState.Vote,
		RaftLog:               raftLog,
		State:                 StateFollower,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		Prs:                   Prs,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	firstIndex, _ := r.RaftLog.storage.FirstIndex()

	ents := make([]*pb.Entry, 0)
	i := int(prevLogIndex + 1 - firstIndex)
	for ; i < len(r.RaftLog.entries); i++ {
		ents = append(ents, &r.RaftLog.entries[i])
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	})

	return true
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
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
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
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
	for peerId := range r.Prs {
		if peerId == r.id {
			r.Prs[peerId] = &Progress{
				Next:  lastIndex + 2,
				Match: lastIndex + 1,
			}
		} else {
			r.Prs[peerId].Next = lastIndex + 1
		}
	}

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	})

	for k := range r.Prs {
		if k != r.id {
			r.sendAppend(k)
		}
	}

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) NewElection() {

	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	for peerId := range r.Prs {
		if peerId != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      peerId,
				Term:    r.Term,
				LogTerm: r.RaftLog.LastTerm(),
				Index:   r.RaftLog.LastIndex(),
			})
		}
	}

}

func (r *Raft) UpdateCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, peerId := range r.Prs {
		match[i] = peerId.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]
	if n > r.RaftLog.committed {
		logTerm, _ := r.RaftLog.Term(n)
		if logTerm == r.Term {
			r.RaftLog.committed = n
			for peerId := range r.Prs {
				if peerId != r.id {
					r.sendAppend(peerId)
				}
			}
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
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

func (r *Raft) StepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.NewElection()
	}
}

func (r *Raft) StepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if r.Term == m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term < r.Term {
			return
		}
		r.votes[m.From] = !m.Reject
		countfor := 0
		mid := len(r.Prs) / 2
		num := len(r.votes)
		for _, v := range r.votes {
			if v {
				countfor++
			}
		}
		if countfor > mid {
			r.becomeLeader()
		} else if num-countfor > mid {
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.NewElection()
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	}
}

func (r *Raft) StepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgPropose:
		lastIndex := r.RaftLog.LastIndex()
		for i, ent := range m.Entries {
			ent.Index = lastIndex + 1 + uint64(i)
			ent.Term = r.Term
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		for peerId := range r.Prs {
			if peerId != r.id {
				r.sendAppend(peerId)
			}
		}
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.Prs[r.id].Match
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	case pb.MessageType_MsgBeat:
		for k := range r.Prs {
			if k != r.id {
				r.sendHeartbeat(k)
			}
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(m.Index)
	firstIndex, _ := r.RaftLog.storage.FirstIndex()

	if m.Index > lastIndex {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   lastIndex + 1,
			Reject:  true,
		})
		return
	}
	if m.Index >= firstIndex {
		if logTerm != m.LogTerm {
			offset := m.Index + 1 - firstIndex
			sliceIndex := sort.Search(int(offset), func(i int) bool {
				return r.RaftLog.entries[i].Term == logTerm
			})
			index := firstIndex + uint64(sliceIndex)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				LogTerm: logTerm,
				Index:   index,
				Reject:  true,
			})
			return
		}
	}
	for i, ent := range m.Entries {
		if ent.Index < firstIndex {
			continue
		}
		if ent.Index > r.RaftLog.LastIndex() {
			for j := i; j < len(m.Entries); j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		} else {
			term, _ := r.RaftLog.Term(ent.Index)
			if term != ent.Term {
				firstIndex, _ := r.RaftLog.storage.FirstIndex()
				index := int(ent.Index - firstIndex)
				r.RaftLog.entries[index] = *ent
				r.RaftLog.entries = r.RaftLog.entries[:index+1]
				if r.RaftLog.stabled >= ent.Index {
					r.RaftLog.stabled = ent.Index - 1
				}
			}

		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	})

}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index
		logTerm := m.LogTerm
		if logTerm != None {
			sliceIndex := sort.Search(len(r.RaftLog.entries), func(i int) bool {
				return r.RaftLog.entries[i].Term > logTerm
			})
			if sliceIndex > 0 && r.RaftLog.entries[sliceIndex-1].Term == logTerm {
				firstIndex, _ := r.RaftLog.storage.FirstIndex()
				index = firstIndex + uint64(sliceIndex)
			}
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.UpdateCommit()
	}
	return
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	logTerm := r.RaftLog.LastTerm()
	//如果本地的最后一条log entry的term更大，则term大的更新，如果term一样大，则log index更大的更新。
	if logTerm > m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	if logTerm == m.LogTerm && lastIndex > m.Index {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
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
