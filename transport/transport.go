package transport

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/valenciusap17/distributed-kvstore-raft/logger"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type HTTPTransport struct {
	id      uint64
	client  *http.Client
	peerMap map[uint64]string
	RecvC   chan *raftpb.Message
	mu      sync.Mutex
}

func NewHTTPTransport(id uint64, peers []string) *HTTPTransport {
	peerMap := make(map[uint64]string)
	for _, peer := range peers {
		idHost := strings.Split(peer, "=")
		id, _ := strconv.ParseInt(idHost[0], 10, 64)
		peerMap[uint64(id)] = idHost[1]
	}
	return &HTTPTransport{
		id:      id,
		client:  &http.Client{},
		peerMap: peerMap,
		RecvC:   make(chan *raftpb.Message, 1024),
	}
}

func (ht *HTTPTransport) GetPeerUrl(id uint64) string {
	if url, ok := ht.peerMap[id]; ok {
		return url
	}
	return ""
}

func (ht *HTTPTransport) AddPeer(newNodeID uint64, newPeerUrl string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if newPeerUrl != "" {
		ht.peerMap[newNodeID] = newPeerUrl
	}

	logger.Log.Infof("Added new peer: Node ID %d, URL: %s", newNodeID, newPeerUrl)
}

func (ht *HTTPTransport) RemovePeer(nodeID uint64) {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	delete(ht.peerMap, nodeID)

	logger.Log.Infof("Removed existing peer: Node ID %d", nodeID)
}

func (ht *HTTPTransport) SendMessages(messages []*raftpb.Message) {
	for _, msg := range messages {
		if *msg.To == ht.id {
			ht.RecvC <- msg
		} else {
			ht.SendMessage(msg)
		}
	}
}

func (ht *HTTPTransport) SendMessage(message *raftpb.Message) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	data, err := proto.Marshal(message)
	if err != nil {
		logger.Log.Warnf("failed ro marshall message: %v", err)
		return
	}

	peerUrl := ht.GetPeerUrl(*message.To)
	if peerUrl == "" {
		logger.Log.Warnf("failed to find peer URL for node %d", *message.To)
		return
	}

	logger.Log.Debugf("seding message from %d to %d of type %s", *message.From, *message.To, message.Type)
	url := fmt.Sprintf("%s/raft", peerUrl)

	resp, err := ht.client.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		logger.Log.Warnf("failed to send message to %s: %v", url, err)
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Log.Warnf("failed to send message to %s, status: %v", url, resp.Status)
	}
}

func (ht *HTTPTransport) Receive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var msg *raftpb.Message
	if err := proto.Unmarshal(body, msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ht.RecvC <- msg
	w.WriteHeader(http.StatusOK)
}
