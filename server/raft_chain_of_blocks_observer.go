package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type chainOfBlocksObserver struct {
	groupName      string
	serverName     string
	nodeId         string
	nc             *nats.Conn
	started        bool
	blockAppliedCh chan *ApplyBlockEvent
	publishCh      chan *BlockEventsBatch
}

type ApplyBlockEvent struct {
	CEIndex           uint64
	BlockIndex        uint64
	BlockChecksum     string
	PreviousChainHash string
	NewChainHash      string
}

type BlockEventsBatch struct {
	ServerName  string
	RaftNodeId  string
	BlockEvents []*ApplyBlockEvent
}

func (o *chainOfBlocksObserver) starting(serverName, raftNodeId string) {

	if o.started {
		panic("started twice")
	}
	o.serverName = serverName
	o.nodeId = raftNodeId
	o.started = true

	go o.publishLoop()
	go o.runLoop()
}

func (o *chainOfBlocksObserver) stopped() {

}

func (o *chainOfBlocksObserver) appliedBlock(ceIndex, blockIndex uint64, blockChecksum, previousChainHash, newChainHash []byte) {
	if !o.started {
		panic("was never started")
	}

	fmt.Printf("✨ apply block %d, block hash: %X chain hash: %X\n", blockIndex, blockChecksum, newChainHash)

	e := &ApplyBlockEvent{
		CEIndex:           ceIndex,
		BlockIndex:        blockIndex,
		BlockChecksum:     fmt.Sprintf("%X", blockChecksum),
		PreviousChainHash: fmt.Sprintf("%X", previousChainHash),
		NewChainHash:      fmt.Sprintf("%X", newChainHash),
	}

	select {
	case o.blockAppliedCh <- e:
		return
	default:
		fmt.Printf("⚠️ Apply block event dropped, channel full")
	}
}

func (o *chainOfBlocksObserver) appliedSnapshot(committedEntryIndex, blocksCount uint64, currentHash string, createdBy string) {
	fmt.Printf("✨ apply snapshot at block %d: chain hash: %s (from: %s)\n", blocksCount, currentHash, createdBy)
}

func (o *chainOfBlocksObserver) runLoop() {

	var currentBatch *BlockEventsBatch

	addToBatch := func(e *ApplyBlockEvent) {
		if currentBatch == nil {
			currentBatch = &BlockEventsBatch{
				ServerName:  o.serverName,
				RaftNodeId:  o.nodeId,
				BlockEvents: make([]*ApplyBlockEvent, 0, 20),
			}
		}
		currentBatch.BlockEvents = append(currentBatch.BlockEvents, e)
		//fmt.Printf("✨ appended event (batch size: %d)\n", len(currentBatch.BlockEvents))

	}

	flush := func() {
		if currentBatch != nil {
			select {
			case o.publishCh <- currentBatch:
				//fmt.Printf("✅ Batch flushed\n")
			default:
				fmt.Printf("⚠️ Block batch dropped, channel full\n")
			}
			currentBatch = nil
		}
	}

	flushTicker := time.NewTicker(5 * time.Second)

	for {
		select {
		//TODO handle quit
		case e := <-o.blockAppliedCh:
			addToBatch(e)
			//add e to current batch
			// if batch size == 10
			//    flush

		case <-flushTicker.C:
			flush()
		}
	}
}

func (o *chainOfBlocksObserver) publishLoop() {

	pubSubj := "chainOfBlocks" + "." + o.groupName + "." + o.serverName

	publish := func(batch *BlockEventsBatch) {
		jsonBatch, err := json.MarshalIndent(batch, "", "  ")
		if err != nil {
			panic(fmt.Sprintf("failed to serialize batch: %s", err))
		}

		for {
			response, err := o.nc.Request(pubSubj, jsonBatch, 3*time.Second)
			if err != nil {
				fmt.Printf("error publishing batch: %s\n", err)
			} else if errors.Is(err, nats.ErrNoResponders) {
				fmt.Printf("%s: %s\n", pubSubj, err)
			} else if bytes.Compare(response.Data, []byte("ok")) != 0 {
				fmt.Printf("unexpected server response: %s\n", response.Data)
			} else {
				// Success
				//fmt.Printf("✅ Batch acknowledged\n")
				break
			}
			time.Sleep(3 * time.Second)
		}
	}

	for {
		select {
		//TODO handle quit
		case batch := <-o.publishCh:
			publish(batch)
		}
	}
}

func newChainOfBlocksObserver(groupName string) (*chainOfBlocksObserver, error) {
	// TODO hardcoded connect string
	nc, err := nats.Connect(
		"nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224,",
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, err
	}
	return &chainOfBlocksObserver{
		groupName:      groupName,
		nc:             nc,
		blockAppliedCh: make(chan *ApplyBlockEvent, 25),
		publishCh:      make(chan *BlockEventsBatch, 25),
	}, nil
}
