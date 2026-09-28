package transport

import (
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/valenciusap17/distributed-kvstore-raft/logger"
	"go.etcd.io/raft/v3/raftpb"
)

type HTTPTransport struct {
	id      uint64
	client  *http.Client
	peerMap map[uint64]string
	Recvc   chan raftpb.Message
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
		Recvc:   make(chan raftpb.Message, 1024),
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
