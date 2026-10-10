package raftnode

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/valenciusap17/distributed-kvstore-raft/kvstore"
	"github.com/valenciusap17/distributed-kvstore-raft/logger"
	"github.com/valenciusap17/distributed-kvstore-raft/transport"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftNode struct {
	Id uint64

	// Node = core Raft isntance for this node, managing consensus and state replication
	Node raft.Node

	// in-memory storage used by the raft library
	// storing logs, snapshots, and metadata
	storage *raft.MemoryStorage

	// handles network communication between nodes inside Raft Cluster
	Transport *transport.HTTPTransport

	// kv store that holds the application state replicated across the cluster
	KvStore kvstore.KeyValueStore

	// hold current configuration state of the cluster, inclusing members and their roles
	ConfState *raftpb.ConfState

	// channel for receiving linearizable read states
	ReadState chan raft.ReadState

	// channel used to signal raft node to shut down gracefully
	stopc chan struct{}

	httpServer *http.Server

	// Directory where snapshots of the Raft state machine are stored for recovery purposes
	dataDir string

	logDir string

	lastSnapshotIndex uint64

	CommitIndex uint64
}

type OperationType int

const (
	OperationAdd OperationType = iota
	OperationDelete
)

var OperationType_Value = map[OperationType]int{
	OperationAdd:    0,
	OperationDelete: 1,
}

var OperationType_Name = map[OperationType]string{
	OperationAdd:    "add",
	OperationDelete: "delete",
}

type LogDataEntry struct {
	Operation OperationType `json:"operation"`
	Key       string        `json:"key"`
	Value     string        `json:"value"`
}

func NewRaftNode(id uint64, kvstore *kvstore.KeyValueStore, initialCluster string, dataDir string, logDir string, join bool) *RaftNode {
	loggerRaft := logrus.New()
	loggerRaft.SetLevel(logrus.DebugLevel)
	raft.SetLogger(loggerRaft)
	snapshot, err := loadSnapshot(dataDir, kvstore)
	if err != nil {
		logger.Log.Fatalf("Error loading snapshot: %v", err)
	}

	storage := raft.NewMemoryStorage()

	var lastSnapshotIndex uint64
	var confState *raftpb.ConfState

	if snapshot != nil {
		if err := storage.ApplySnapshot(snapshot); err != nil {
			logger.Log.Fatalf("Error applying snapshot: %v", err)
		}
		confState = snapshot.Metadata.ConfState
		lastSnapshotIndex = *snapshot.Metadata.Index
	}

	if err := loadRaftLog(logDir, storage); err != nil {
		logger.Log.Fatalf("Error loading logs: %v", err)
	}

	c := &raft.Config{
		ID:                        id,
		ElectionTick:              100,
		HeartbeatTick:             10,
		Storage:                   storage,
		MaxInflightMsgs:           256,
		MaxSizePerMsg:             1024 * 1024,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	peerUrls := strings.Split(initialCluster, ",")
	var raftPeers []raft.Peer
	for i, _ := range peerUrls {
		raftPeers = append(raftPeers, raft.Peer{ID: uint64(i + 1)})
	}

	var n raft.Node
	if join {
		n = raft.RestartNode(c)
	} else {
		n = raft.StartNode(c, raftPeers)
	}

	tp := transport.NewHTTPTransport(id, peerUrls)

	rn := &RaftNode{
		Id:                id,
		Node:              n,
		storage:           storage,
		Transport:         tp,
		KvStore:           *kvstore,
		stopc:             make(chan struct{}),
		ReadState:         make(chan raft.ReadState),
		dataDir:           dataDir,
		logDir:            logDir,
		lastSnapshotIndex: lastSnapshotIndex,
		ConfState:         confState,
	}

	return rn
}

func (rn *RaftNode) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopc:
			return
		case rd := <-rn.Node.Ready():
			rn.writeReadStates(rd)

			if err := rn.storage.Append(rd.Entries); err != nil {
				logger.Log.Fatal(err)
			}

			rn.Transport.SendMessages(rd.Messages)

			if len(rd.CommittedEntries) > 0 {
				rn.CommitIndex = *rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
				rn.appendToLog(rd.CommittedEntries)
			}
			rn.processComittedEntries(rd)
			rn.Node.Advance()
		case msg := <-rn.Transport.RecvC:
			err := rn.Node.Step(context.Background(), msg)
			if err != nil {
				return
			}
		case <-ticker.C:
			rn.Node.Tick()
		}
	}
}

func (rn *RaftNode) writeReadStates(rd raft.Ready) {
	for _, rs := range rd.ReadStates {
		rn.ReadState <- rs
	}
}

func (rn *RaftNode) processComittedEntries(rd raft.Ready) {
	for _, entry := range rd.CommittedEntries {
		if *entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
			var logDataEntry LogDataEntry
			if err := json.Unmarshal(entry.Data, &logDataEntry); err == nil {
				if logDataEntry.Operation == OperationAdd {
					rn.KvStore.Set(logDataEntry.Key, logDataEntry.Value)
				} else if logDataEntry.Operation == OperationDelete {
					rn.KvStore.Delete(logDataEntry.Key)
				}
			}
		}
	}
}

func (rn *RaftNode) appendToLog(entries []*raftpb.Entry) {
	for _, entry := range entries {
		err := appendToLogFile(rn.logDir, entry)
		if err != nil {
			return
		}
	}
}
