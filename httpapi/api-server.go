package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
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

func (as *ApiServer) handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// if leader no need to do linearized check
	if as.RaftNode.Node.Status().Lead == as.RaftNode.Id {
		value, ok := as.RaftNode.KvStore.Get(key)

		if !ok {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}

		json.NewEncoder(w).Encode(map[string]string{key: value})
		return
	}

	reqCtx := []byte(key)
	ctx := context.TODO()
	err := as.RaftNode.Node.ReadIndex(ctx, reqCtx)
	if err != nil {
		http.Error(w, "Failed to initiate ReadIndex", http.StatusInternalServerError)
		return
	}

	rs := <-as.RaftNode.ReadState

	if string(rs.RequestCtx) != key {
		http.Error(w, "ReadIndex mismatch", http.StatusInternalServerError)
		return
	}

	// wait for 10 seconds
	wait := 0
	waitOver := false
	for as.RaftNode.CommitIndex < rs.Index {
		if wait > 10 {
			waitOver = true
			break
		}
		time.Sleep(1 * time.Second)
		wait += 1
	}

	if waitOver {
		http.Error(w, "Key not found", http.StatusInternalServerError)
		return
	}

	value, ok := as.RaftNode.KvStore.Get(key)

	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{key: value})

}
