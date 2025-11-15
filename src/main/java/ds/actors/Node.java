package ds.actors;

import ds.model.Request;
import ds.model.Types.*;
import ds.config.Settings;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Node actor
public class Node extends AbstractActor {

    // Node fields
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private final Map<Integer, DataItem> data;
    private final Map<Integer, ActorRef> peers;
    private final Map<Integer, Request> pendingRequests;

    // Constructors
    public Node(int id) {
        this.id = id;
        this.data = new HashMap<>();
        this.peers = new HashMap<>();
        this.pendingRequests = new HashMap<>();
    }

    public Node(int id, Map<Integer, ActorRef> peers) {
        this.id = id;
        this.data = new HashMap<>();
        this.peers = new HashMap<>(peers);
        this.pendingRequests = new HashMap<>();
    }

    public Node(int id, Map<Integer, ActorRef> peers, Map<Integer, DataItem> data) {
        this.id = id;
        this.data = new HashMap<>(data);
        this.peers = new HashMap<>(peers);
        this.pendingRequests = new HashMap<>();
    }

    private List<Integer> findReplicaNodes(int key, List<Integer> NodeIds) {
        List<Integer> replicas = new ArrayList<>();
        List<Integer> sortedNodeIds = new ArrayList<>(NodeIds);
        Collections.sort(sortedNodeIds);
        int n = sortedNodeIds.size();
        if (n == 0) {
            return replicas;
        }
        int startIndex = 0;
        for (int i = 0; i < n; i++) {
            if (sortedNodeIds.get(i) >= key) {
                startIndex = i;
                break;
            }
        }
        for (int i = 0; i < Math.min(Settings.N, n); i++) {
            int index = (startIndex + i) % n;
            replicas.add(sortedNodeIds.get(index));
        }
        return replicas;
    }

    private void get(Get msg) {
        log.debug("Node[{}]: Received GET request for key {}", id, msg.key());
        // TODO: Implement distributed GET operation with quorum
    }
    
    private void update(Update msg) {
        data.put(msg.key(), new DataItem(msg.value()));
        log.info("Node[{}]: Updated key {} with value {}", id, msg.key(), msg.value());
    }
    private void setPeers(SetPeers msg) {
        peers.clear();
        peers.putAll(msg.peers());
        log.info("Node[{}]: Peers set to {}", id, peers.keySet());
    }
    private void updatePeer(UpdatePeer msg) {
        peers.put(msg.id(), msg.peer());
        log.info("Node[{}]: Peer {} updated", id, msg.id());
    }
    private void print(Print msg) {
        log.info("Node[{}]: Data store content: {}", id, data);
    }
    private void printPeers(PrintPeers msg) {
        log.info("Node[{}]: Known peers: {}", id, peers.keySet());
    }

   @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Get.class,this::get)
                .match(Update.class, this::update)
                .match(SetPeers.class, this::setPeers)
                .match(UpdatePeer.class, this::updatePeer)
                .match(Print.class, this::print)
                .match(PrintPeers.class, this::printPeers)
                .build();
    }
}