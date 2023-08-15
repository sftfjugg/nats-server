package server

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"math/rand"
	"path/filepath"
	"sync"
	"time"
)

func JoinRaftGroup(s *Server, name string) error {
	sm, err := newRaftBlahChainStateMachine(name, s)
	if err != nil {
		return err
	}

	go func() {
		time.Sleep(3 * time.Second)
		rng := rand.New(rand.NewSource(rand.Int63()))
		valueBuf := make([]byte, 10)
		for {
			time.Sleep(1 * time.Second)
			rng.Read(valueBuf)
			valueString := fmt.Sprintf("%X", valueBuf)
			sm.propose([]byte(valueString))
		}
	}()
	return nil
}

type RaftChainStateMachine struct {
	sync.Mutex
	groupName               string
	server                  *Server
	raftNode                RaftNode
	blockCount              uint64
	hash                    hash.Hash32
	blocksSinceLastSnapshot uint64
}

func newRaftBlahChainStateMachine(name string, s *Server) (*RaftChainStateMachine, error) {

	storeDir := filepath.Join(s.StoreDir(), globalAccountName, defaultStoreDirName, name)

	fs, err := newFileStoreWithCreated(
		FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, SyncInterval: 10 * time.Second},
		StreamConfig{Name: name, Storage: FileStorage},
		time.Now().UTC(),
		s.jsKeyGen(name),
	)
	if err != nil {
		return nil, err
	}

	fs.registerServer(s)

	cfg := &RaftConfig{Name: name, Store: storeDir, Log: fs}

	var bootstrap bool
	if ps, err := readPeerState(storeDir); err != nil {
		s.Noticef("JetStream cluster bootstrapping")
		bootstrap = true
		peers := s.ActivePeers()
		s.Debugf("JetStream cluster initial peers: %+v", peers)
		if err := s.bootstrapRaftNode(cfg, peers, false); err != nil {
			return nil, err
		}
	} else {
		s.Noticef("JetStream cluster recovering state")
		if err := writePeerState(storeDir, ps); err != nil {
			return nil, err
		}
	}

	// Start up our meta node.
	n, err := s.startRaftNode(globalAccountName, cfg)
	if err != nil {
		return nil, err
	}

	if bootstrap {
		err := n.Campaign()
		if err != nil {
			return nil, err
		}
	}

	sm := &RaftChainStateMachine{
		groupName:  name,
		server:     s,
		raftNode:   n,
		blockCount: 0,
		hash:       crc32.NewIEEE(),
	}

	sm.log("Start raft SM for group %s (storeDir: %s)", name, storeDir)

	go sm.runLoop()

	return sm, nil
}

func (sm *RaftChainStateMachine) runLoop() {
	qch, lch, aq := sm.raftNode.QuitC(), sm.raftNode.LeadChangeC(), sm.raftNode.ApplyQ()

	snapshotTicker := time.NewTicker(15 * time.Second)

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
				sm.applyEntries(ce)
			}
			aq.recycle(&ces)

		case isLeader := <-lch:
			sm.leaderChange(isLeader)
		case <-snapshotTicker.C:
			sm.snapshot()
		}
	}
}

func (sm *RaftChainStateMachine) applyEntries(ce *CommittedEntry) {
	sm.Lock()
	defer sm.Unlock()

	if ce == nil {
		sm.log("Nil committed entries")
		return
	}

	sm.log(
		"Applying committed entries batch #%d (%d sub-entries)",
		ce.Index,
		len(ce.Entries),
	)

	for i, entry := range ce.Entries {
		sm.log("  [%d/%d]: %s (%dB)", i+1, len(ce.Entries), entry.Type, len(entry.Data))
		switch entry.Type {
		case EntryAddPeer:
			sm.addPeer(string(entry.Data))
		case EntryNormal:
			sm.applyNext(ce.Index, entry.Data)
		default:
			panic(fmt.Sprintf("unhandled entry type: %s", entry.Type))
		}
	}
}

func (sm *RaftChainStateMachine) leaderChange(leader bool) {
	sm.Lock()
	defer sm.Unlock()
	sm.log("Leader change (self? %v)", leader)
}

func (sm *RaftChainStateMachine) log(format string, v ...interface{}) {
	sm.server.Logger().Noticef("🌼 "+format, v...)
}

type blockChainSnapshot struct {
	HashData    []byte
	BlocksCount uint64
}

func (sm *RaftChainStateMachine) snapshot() {
	sm.Lock()
	defer sm.Unlock()

	if sm.blocksSinceLastSnapshot == 0 {
		sm.log("Skip snapshot, no new blocks")
		return
	}

	sm.log(
		"Creating snapshot at block %d (%d blocks since last snapshot)",
		sm.blockCount,
		sm.blocksSinceLastSnapshot,
	)

	serializedHash, err := sm.hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal hash: %s", err))
	}

	snapshot := blockChainSnapshot{
		HashData:    serializedHash,
		BlocksCount: sm.blockCount,
	}

	var snapshotBuf bytes.Buffer
	err = binary.Write(&snapshotBuf, binary.BigEndian, &snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to serialize snapshot: %s", err))
	}

	err = sm.raftNode.InstallSnapshot(snapshotBuf.Bytes())
	if err != nil {
		panic(fmt.Sprintf("failed to install snapshot: %s", err))
	}

	sm.blocksSinceLastSnapshot = 0
}

func (sm *RaftChainStateMachine) propose(bytes []byte) {
	sm.Lock()
	defer sm.Unlock()
	err := sm.raftNode.Propose(bytes)
	if err != nil {
		sm.log("Propose error: %s", err)
	}
}

// Lock held
func (sm *RaftChainStateMachine) addPeer(peerName string) {
	sm.log("Add peer: %s", peerName)
	sm.server.mu.RLock()
	defer sm.server.mu.RUnlock()
	sm.log("  Active peers: %v", sm.server.ActivePeers())
}

// Lock held
func (sm *RaftChainStateMachine) applyNext(entryIndex uint64, data []byte) {
	// Hash entry index
	err := binary.Write(sm.hash, binary.BigEndian, entryIndex)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash entry index: %s", err))
	}

	// Hash entry data
	written, err := sm.hash.Write(data)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash block data: %s", err))
	} else if written != len(data) {
		panic(fmt.Sprintf("Partially read block data: %d != %d", written, len(data)))
	}

	sm.blockCount += 1
	sm.blocksSinceLastSnapshot += 1

	sm.log("Hash after %d blocks: %X", sm.blockCount, sm.hash.Sum(nil))
}
