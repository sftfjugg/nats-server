package server

import (
	"os"
	"time"
)

func JoinRaftGroup(s *Server, name string) error {
	_, err := newRaftBlahChainStateMachine(name, s)
	if err != nil {
		return err
	}
	return nil
}

type RaftChainStateMachine struct {
	groupName string
	server    *Server
	raftNode  RaftNode
}

func newRaftBlahChainStateMachine(name string, s *Server) (*RaftChainStateMachine, error) {
	walDir, err := os.MkdirTemp(s.StoreDir(), "raft-wal_"+s.Name()+"_"+name+"_*")
	if err != nil {
		return nil, err
	}

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: walDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, SyncInterval: 5 * time.Minute},
		StreamConfig{Name: name, Storage: FileStorage},
	)
	if err != nil {
		return nil, err
	}

	storeDir, err := os.MkdirTemp(s.StoreDir(), "raft-store_"+s.Name()+"_"+name+"_*")
	if err != nil {
		return nil, err
	}

	knownPeers := s.ActivePeers()
	cfg := &RaftConfig{Name: name, Store: storeDir, Log: fs}
	err = s.bootstrapRaftNode(cfg, knownPeers, false)
	if err != nil {
		return nil, err
	}
	n, err := s.startRaftNode(globalAccountName, cfg)
	if err != nil {
		return nil, err
	}

	sm := &RaftChainStateMachine{
		groupName: name,
		server:    s,
		raftNode:  n,
	}

	sm.log("Start raft SM for group %s (walDir: %s, storeDir: %s)", name, walDir, storeDir)

	sm.run()

	return sm, nil
}

func (sm *RaftChainStateMachine) run() {
	go sm.runLoop()
}

func (sm *RaftChainStateMachine) runLoop() {
	qch, lch, aq := sm.raftNode.QuitC(), sm.raftNode.LeadChangeC(), sm.raftNode.ApplyQ()

	for {
		select {
		case <-sm.server.quitCh:
			sm.log("Shutting down")
			return
		case <-qch:
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, ce := range ces {
				sm.applyEntry(ce)
			}
			aq.recycle(&ces)

		case isLeader := <-lch:
			sm.leaderChange(isLeader)
		}
	}

}

func (sm *RaftChainStateMachine) applyEntry(ce *CommittedEntry) {
	if ce == nil {
		sm.log("Nil committed entries")
		return
	}

	sm.log(
		"Applying committed entry #%d (%d sub-entries)",
		ce.Index,
		len(ce.Entries),
	)

	for i, entry := range ce.Entries {
		sm.log("  [%d/%d]: %s (%dB)", i+1, len(ce.Entries), entry.Type, len(entry.Data))
		switch entry.Type {
		case EntryAddPeer:
			sm.addPeer(string(entry.Data))
		}
	}
}

func (sm *RaftChainStateMachine) leaderChange(leader bool) {
	sm.log("Leader change (self? %v)", leader)
}

func (sm *RaftChainStateMachine) log(format string, v ...interface{}) {
	sm.server.Logger().Noticef("🌼 "+format, v...)
}

func (sm *RaftChainStateMachine) addPeer(peerName string) {
	sm.log("Add peer: %s", peerName)
}
