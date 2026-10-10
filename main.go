package distributedkvstoreraft

import (
	"flag"
	"strconv"

	"github.com/valenciusap17/distributed-kvstore-raft/httpapi"
	"github.com/valenciusap17/distributed-kvstore-raft/kvstore"
	"github.com/valenciusap17/distributed-kvstore-raft/raftnode"
)

func main() {
	id := flag.Uint64("id", 1, "node ID")
	clientListenUrl := flag.String("listen-client-url", "http://localhost:2379", "client listen url")
	peerListenUrl := flag.String("listen-peer-url", "http://localhost:2380", "peer listen url")
	initialCluster := flag.String("initial-cluster", "", "initial cluster configuration")
	join := flag.Bool("join", false, "join an existing cluster")
	dataDir := flag.String("data-dir", "", "snapshot dir")
	keyValueStorePath := flag.String("key-store-dir", "", "key store path")
	logDir := dataDir
	flag.Parse()

	keyValueFile := *keyValueStorePath + "/data-node" + strconv.FormatUint(*id, 10) + ".json"
	jsonStore := kvstore.NewJsonStore(keyValueFile)
	kvstore := kvstore.NewKeyValueStore(jsonStore)

	rn := raftnode.NewRaftNode(*id, kvstore, *initialCluster, *dataDir, *logDir, *join)

	apiServer := httpapi.ApiServer{RaftNode: rn}
	peerServer := httpapi.ApiServer{RaftNode: rn}

	go apiServer.ServeHTTP(*clientListenUrl)
	go peerServer.ServeHTTP(*peerListenUrl)
	go rn.Run()

	select {}
}
