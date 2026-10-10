package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/valenciusap17/distributed-kvstore-raft/logger"
	"github.com/valenciusap17/distributed-kvstore-raft/raftnode"
)

type ApiServer struct {
	RaftNode *raftnode.RaftNode
}

type createKeyValueRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func StripHTTPString(url string) string {
	return strings.TrimPrefix(url, "http://")
}

func (as *ApiServer) ServeHTTP(clientListenUrl string) {
	r := http.NewServeMux()
	r.HandleFunc("PUT /kv", as.handleSet)
	r.HandleFunc("GET /kv/{key}", as.handleSet)
	r.HandleFunc("DELETE /kv/{key}", as.handleSet)

	clientAddr := StripHTTPString(clientListenUrl)
	logger.Log.Printf("Starting client HTTP server on %s", clientAddr)
	if err := http.ListenAndServe(clientAddr, r); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Log.Fatalf("ListenAndServe(): %v", err)
	}

}

func (as *ApiServer) handleSet(w http.ResponseWriter, r *http.Request) {
	var keyValue createKeyValueRequest
	if err := json.NewDecoder(r.Body).Decode(&keyValue); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	logDataEntry := raftnode.LogDataEntry{
		Operation: raftnode.OperationAdd,
		Key:       keyValue.Key,
		Value:     keyValue.Value,
	}

	data, err := json.Marshal(logDataEntry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = as.RaftNode.Node.Propose(ctx, data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "Propose operation timed out", http.StatusRequestTimeout)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
