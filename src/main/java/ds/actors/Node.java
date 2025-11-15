package ds.actors;
import ds.data_structures.DataItem;
import ds.data_structures.Request;
import ds.data_structures.Settings;
import ds.data_structures.Messages.Get;
import ds.data_structures.Messages.Update;
import ds.data_structures.Messages.SetPeers;
import ds.data_structures.Messages.UpdatePeer;
import ds.data_structures.Messages.Print;
import ds.data_structures.Messages.PrintPeers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;


import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

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
        
    }
    private void update(Update msg) {
        // Implementation of getting data from replicas
    }
    private void setPeers(SetPeers msg) {
        // Implementation of setting peers
    }
    private void updatePeer(UpdatePeer msg) {
        // Implementation of updating a peer
    }
    private void print(Print msg) {
        // Implementation of printing current data store
    }
    private void printPeers(PrintPeers msg) {
        // Implementation of printing peers
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