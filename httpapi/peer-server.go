package httpapi

import (
	"net/http"

	"github.com/valenciusap17/distributed-kvstore-raft/logger"
	"github.com/valenciusap17/distributed-kvstore-raft/raftnode"
)

type PeerServer struct {
	RaftNode *raftnode.RaftNode
}

func (ps *PeerServer) ServeHTTP(peerListenUrl string) {
	r := http.NewServeMux()
	r.HandleFunc("POST /raft", ps.RaftNode.Transport.Receive)

	peerAddr := StripHTTPString(peerListenUrl)
	logger.Log.Printf("Starting peer HTTP server on %s", peerAddr)
	if err := http.ListenAndServe(peerAddr, r); err != nil && err != http.ErrServerClosed {
		logger.Log.Fatalf("ListenAndServe(): %v", err)
	}
}
