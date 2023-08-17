package server

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func JoinChainOfBlocksGroup(s *Server, name string) error {

	observer, err := newChainOfBlocksObserver(name)
	if err != nil {
		return fmt.Errorf("failed to create observer for group %s: %w", name, err)
	}

	sm, err := newChainOfBlocksStateMachine(name, s, observer)
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
			err := sm.propose([]byte(valueString))
			if errors.Is(err, errNotLeader) {
				// Backoff if not leader, no point in keep trying
				time.Sleep(5 * time.Second)
			} else if err != nil {
				fmt.Printf("‼️ Propose error: %s", err)
			}
		}
	}()
	return nil
}

type ChainOfBlocksStateMachine struct {
	sync.Mutex
	groupName                 string
	server                    *Server
	raftNode                  RaftNode
	blockCount                uint64
	hash                      hash.Hash32
	blocksSinceLastSnapshot   uint64
	currentCommitEntriesIndex uint64
	observer                  *chainOfBlocksObserver
}

func newChainOfBlocksStateMachine(name string, s *Server, observer *chainOfBlocksObserver) (*ChainOfBlocksStateMachine, error) {

	var storeDir string
	if s.StoreDir() != _EMPTY_ {
		storeDir = filepath.Join(s.StoreDir(), globalAccountName, defaultStoreDirName, name)
	} else {
		storeDir = filepath.Join(os.TempDir(), "raft-test", s.Name(), globalAccountName, defaultStoreDirName, name)
	}

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

	sm := &ChainOfBlocksStateMachine{
		groupName:  name,
		server:     s,
		raftNode:   n,
		blockCount: 0,
		hash:       crc32.NewIEEE(),
		observer:   observer,
	}

	sm.log("Start raft SM for group %s (storeDir: %s)", name, storeDir)

	go sm.runLoop()

	return sm, nil
}

func (sm *ChainOfBlocksStateMachine) runLoop() {
	sm.observer.starting(sm.server.Name(), sm.raftNode.ID())
	defer sm.observer.stopped()

	qch, lch, aq := sm.raftNode.QuitC(), sm.raftNode.LeadChangeC(), sm.raftNode.ApplyQ()

	snapshotTicker := time.NewTicker(15 * time.Second)

	for {
		select {
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

func (sm *ChainOfBlocksStateMachine) applyEntries(ce *CommittedEntry) {
	sm.Lock()
	defer sm.Unlock()

	if ce == nil {
		sm.log("Nil committed entries")
		return
	}

	if ce.Index <= sm.currentCommitEntriesIndex {
		sm.log("Skip committed entries #%d, already applied", ce.Index)
		return
	}

	sm.currentCommitEntriesIndex = ce.Index

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
			sm.applyBlock(ce.Index, entry.Data)
		case EntrySnapshot:
			sm.loadSnapshot(ce.Index, entry.Data)
		default:
			panic(fmt.Sprintf("unhandled entry type: %s", entry.Type))
		}
	}

}

func (sm *ChainOfBlocksStateMachine) leaderChange(leader bool) {
	sm.Lock()
	defer sm.Unlock()
	sm.log("Leader change (self? %v)", leader)
}

func (sm *ChainOfBlocksStateMachine) log(format string, v ...interface{}) {
	sm.server.Logger().Noticef("🌼 "+format, v...)
}

type blockChainSnapshot struct {
	HashData            []byte
	BlocksCount         uint64
	CurrentHash         string
	CommittedEntryIndex uint64
}

func (sm *ChainOfBlocksStateMachine) snapshot() {
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
		HashData:            serializedHash,
		BlocksCount:         sm.blockCount,
		CurrentHash:         fmt.Sprintf("%X", sm.hash.Sum(nil)),
		CommittedEntryIndex: sm.currentCommitEntriesIndex,
	}

	var snapshotBuf bytes.Buffer
	err = gob.NewEncoder(&snapshotBuf).Encode(&snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to serialize snapshot: %s", err))
	}

	err = sm.raftNode.InstallSnapshot(snapshotBuf.Bytes())
	if err != nil {
		panic(fmt.Sprintf("failed to install snapshot: %s", err))
	}

	sm.blocksSinceLastSnapshot = 0

	sm.log("Created snapshot: %+v", snapshot)
}

func (sm *ChainOfBlocksStateMachine) propose(bytes []byte) error {
	sm.Lock()
	defer sm.Unlock()
	return sm.raftNode.Propose(bytes)
}

// Lock held
func (sm *ChainOfBlocksStateMachine) addPeer(peerName string) {
	sm.log("Add peer: %s", peerName)
	sm.server.mu.RLock()
	defer sm.server.mu.RUnlock()
	sm.log("  Active peers: %v", sm.server.ActivePeers())
}

// Lock held
func (sm *ChainOfBlocksStateMachine) applyBlock(entryIndex uint64, data []byte) {

	// Hash data by itself to obtain block hash
	blockHash := crc32.NewIEEE()
	written, err := blockHash.Write(data)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash block data (by itself): %s", err))
	} else if written != len(data) {
		panic(fmt.Sprintf("Partially read block data: %d != %d", written, len(data)))
	}
	blockChecksum := blockHash.Sum(nil)

	previousChainHash := sm.hash.Sum(nil)

	// Hash data to obtain next chain hash
	written, err = sm.hash.Write(data)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash block data: %s", err))
	} else if written != len(data) {
		panic(fmt.Sprintf("Partially read block data: %d != %d", written, len(data)))
	}

	currentChainHash := sm.hash.Sum(nil)

	sm.blockCount += 1
	sm.blocksSinceLastSnapshot += 1

	sm.log(
		"Ingested block %X chain hash after %d blocks: %X -> %X",
		blockChecksum,
		sm.blockCount,
		previousChainHash,
		currentChainHash,
	)

	sm.observer.appliedBlock(entryIndex, sm.blockCount, blockChecksum, previousChainHash, currentChainHash)
}

// Lock held
func (sm *ChainOfBlocksStateMachine) loadSnapshot(entryIndex uint64, snapshotData []byte) {
	snapshot := blockChainSnapshot{}
	err := gob.NewDecoder(bytes.NewBuffer(snapshotData)).Decode(&snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to load snapshot: %s", err))
	}

	err = sm.hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(snapshot.HashData)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshall hash in snapshot: %s", err))
	}

	currentHash := fmt.Sprintf("%X", sm.hash.Sum(nil))
	if currentHash != snapshot.CurrentHash {
		panic(fmt.Sprintf("Snapshot hash mismatch: %s != %s", currentHash, snapshot.CurrentHash))
	}

	sm.blockCount = snapshot.BlocksCount
	sm.blocksSinceLastSnapshot = 0
	sm.currentCommitEntriesIndex = snapshot.CommittedEntryIndex

	sm.log("Installed snapshot: %+v", snapshot)
}
