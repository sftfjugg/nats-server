// Copyright 2021-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestNRGSimple(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	// Do several state transitions.
	rg.randomMember().(*stateAdder).proposeDelta(11)
	rg.randomMember().(*stateAdder).proposeDelta(11)
	rg.randomMember().(*stateAdder).proposeDelta(-22)
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, 0)
}

func TestNRGSnapshotAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	var expectedTotal int64

	leader := rg.leader().(*stateAdder)
	sm := rg.nonLeader().(*stateAdder)

	for i := 0; i < 1000; i++ {
		delta := rand.Int63n(222)
		expectedTotal += delta
		leader.proposeDelta(delta)

		if i == 250 {
			// Let some things catchup.
			time.Sleep(50 * time.Millisecond)
			// Snapshot leader and stop and snapshot a member.
			leader.snapshot(t)
			sm.snapshot(t)
			sm.stop()
		}
	}
	// Restart.
	sm.restart()
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, expectedTotal)
}

func TestNRGAppendEntryEncode(t *testing.T) {
	ae := &appendEntry{
		term:   1,
		pindex: 0,
	}

	// Test leader should be _EMPTY_ or exactly idLen long
	ae.leader = "foo_bar_baz"
	_, err := ae.encode(nil)
	require_Error(t, err, errLeaderLen)

	// Empty ok (noLeader)
	ae.leader = noLeader // _EMPTY_
	_, err = ae.encode(nil)
	require_NoError(t, err)

	ae.leader = "DEREK123"
	_, err = ae.encode(nil)
	require_NoError(t, err)

	// Buffer reuse
	var rawSmall [32]byte
	var rawBigger [64]byte

	b := rawSmall[:]
	ae.encode(b)
	if b[0] != 0 {
		t.Fatalf("Expected arg buffer to not be used")
	}
	b = rawBigger[:]
	ae.encode(b)
	if b[0] == 0 {
		t.Fatalf("Expected arg buffer to be used")
	}

	// Test max number of entries.
	for i := 0; i < math.MaxUint16+1; i++ {
		ae.entries = append(ae.entries, &Entry{EntryNormal, nil})
	}
	_, err = ae.encode(b)
	require_Error(t, err, errTooManyEntries)
}

func TestNRGAppendEntryDecode(t *testing.T) {
	ae := &appendEntry{
		leader: "12345678",
		term:   1,
		pindex: 0,
	}
	for i := 0; i < math.MaxUint16; i++ {
		ae.entries = append(ae.entries, &Entry{EntryNormal, nil})
	}
	buf, err := ae.encode(nil)
	require_NoError(t, err)

	// Truncate buffer first.
	var node *raft
	short := buf[0 : len(buf)-1024]
	_, err = node.decodeAppendEntry(short, nil, _EMPTY_)
	require_Error(t, err, errBadAppendEntry)

	for i := 0; i < 100; i++ {
		b := copyBytes(buf)
		// modifying the header (idx < 42) will not result in an error by decodeAppendEntry
		bi := 42 + rand.Intn(len(b)-42)
		if b[bi] != 0 && bi != 40 {
			b[bi] = 0
			_, err = node.decodeAppendEntry(b, nil, _EMPTY_)
			require_Error(t, err, errBadAppendEntry)
		}
	}
}

func TestRaftChain(t *testing.T) {
	const numBlocks = 5
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	for iteration := 1; iteration <= 10; iteration++ {
		// Do several state transitions.
		for i := 0; i < numBlocks; i++ {
			rg.randomMember().(*raftChainStateMachine).proposeBlock()
		}

		// Wait on participants to converge
		checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
			expectedBlocks := uint64(iteration * numBlocks)
			var otherNode, otherNodeHash string
			for i, sm := range rg {
				stateMachine := sm.(*raftChainStateMachine)
				blocksCount, currentHash := stateMachine.getCurrentHash()
				name := fmt.Sprintf(
					"%s/%s",
					stateMachine.server().Name(),
					stateMachine.node().ID(),
				)
				if blocksCount != expectedBlocks {
					return fmt.Errorf(
						"node %s has applied %d/%d blocks",
						name,
						blocksCount,
						expectedBlocks,
					)
				}
				// Block count matches expected, check hash
				if i == 0 {
					// Save first for comparison
					otherNode = name
					otherNodeHash = currentHash
				} else if currentHash != otherNodeHash {
					return fmt.Errorf(
						"node %s hash: %q different from node %s hash: %q",
						name,
						currentHash,
						otherNode,
						otherNodeHash,
					)
				}
			}
			fmt.Printf("✅ %d nodes converged on hash %s after %d blocks\n", 3, otherNodeHash, expectedBlocks)
			return nil
		})

		// Restart one of the participants
		stateMachine := rg.randomMember()
		stateMachine.stop()
		stateMachine.restart()
	}
}

func TestRaftChain2(t *testing.T) {
	const numBlocks = 5
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	activeNodes := make([]stateMachine, 3)
	stoppedNodes := make([]stateMachine, 3)
	activeNodes[0] = rg[0]
	activeNodes[1] = rg[1]
	activeNodes[2] = rg[2]

	rng := rand.New(rand.NewSource(12345))

	type Operation int
	const (
		STOP Operation = iota
		RESTART
		SNAPSHOT
		PROPOSE
		PAUSE
	)

	probabilities := []Operation{
		STOP,
		RESTART,
		RESTART,
		RESTART,
		SNAPSHOT,
		SNAPSHOT,
		PROPOSE,
		PROPOSE,
		PROPOSE,
		PROPOSE,
		PAUSE,
		PAUSE,
	}

	for iteration := 0; iteration < 1000; iteration++ {
		r := rng.Intn(len(probabilities))
		switch probabilities[r] {
		case STOP:
			// Stop a node
			i := rng.Intn(3)
			if activeNodes[i] != nil {
				stoppedNodes[i] = activeNodes[i]
				activeNodes[i] = nil
				stoppedNodes[i].stop()
			}

		case RESTART:
			// Restart a stopped node
			i := rng.Intn(3)
			if stoppedNodes[i] != nil {
				activeNodes[i] = stoppedNodes[i]
				stoppedNodes[i] = nil
				activeNodes[i].restart()
			}
		case SNAPSHOT:
			// Take a snapshot
			i := rng.Intn(3)
			if activeNodes[i] != nil {
				activeNodes[i].(*raftChainStateMachine).snapshot()
			}
		case PROPOSE:
			// Propose a block
			i := rng.Intn(3)
			if activeNodes[i] != nil {
				activeNodes[i].(*raftChainStateMachine).proposeBlock()
			}
		case PAUSE:
			time.Sleep(time.Duration(rng.Intn(200)) * time.Millisecond)
		}

		fmt.Printf("___STATE___\n")
		for i := 0; i < 3; i++ {
			if activeNodes[i] != nil {
				activeNode := activeNodes[i]
				blocksCount, blockHash := activeNode.(*raftChainStateMachine).getCurrentHash()
				fmt.Printf(" - %s: %s (%d blocks)\n", activeNode.server().Name(), blockHash, blocksCount)
			} else {
				stoppedNode := stoppedNodes[i]
				fmt.Printf(" - %s: *stopped*\n", stoppedNode.server().Name())
			}
		}

	}
}
