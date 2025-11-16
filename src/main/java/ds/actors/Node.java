package ds.actors;

import ds.model.Request;
import ds.model.RequestType;
import ds.model.Types.*;
import ds.config.Settings;

import akka.actor.AbstractActor;
import akka.actor.Props;
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
    private final Map<Integer, Request> requestsLedger;

    // Constructors
    public Node(int id) {
        this.id = id;
        this.data = new HashMap<>();
        this.peers = new HashMap<>();
        this.requestsLedger = new HashMap<>();
    }

    public Node(int id, Map<Integer, ActorRef> peers) {
        this.id = id;
        this.data = new HashMap<>();
        this.peers = new HashMap<>(peers);
        this.requestsLedger = new HashMap<>();
    }

    public Node(int id, Map<Integer, ActorRef> peers, Map<Integer, DataItem> data) {
        this.id = id;
        this.data = new HashMap<>(data);
        this.peers = new HashMap<>(peers);
        this.requestsLedger = new HashMap<>();
    }

    private List<Integer> findReplicaNodesIds(int key, List<Integer> NodeIds) {
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

    private void prepareReplicasAndQuorum(int key, ArrayList<ActorRef> nodeRefs, ArrayList<DataItem> quorum) {
        List<Integer> nodeIds = new ArrayList<>(peers.keySet());
        nodeIds.add(id);
        List<Integer> replicaIds = findReplicaNodesIds(key, nodeIds);
        
        for (Integer nodeId : replicaIds) {
            if (nodeId == id) {
                DataItem localData = data.get(key);
                if (localData != null) {
                    quorum.add(localData);
                }
            } else {
                nodeRefs.add(peers.get(nodeId));
            }
        }
    }

    private int generateOperationId() {
        return requestsLedger.size() + 1;
    }

    private void handleClientGetRequest(ClientGetRequest msg) {
        log.debug("Node[{}]: Received client GET request for key {}", id, msg.key());
        
        ArrayList<ActorRef> nodeRefs = new ArrayList<>();
        ArrayList<DataItem> quorum = new ArrayList<>();
        prepareReplicasAndQuorum(msg.key(), nodeRefs, quorum);
        
        int op_id = generateOperationId();
        requestsLedger.put(op_id, new Request(getSender(), RequestType.GET));
        getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, msg.key()));
    }
    
    private void handleClientUpdateRequest(ClientUpdateRequest msg) {
        log.debug("Node[{}]: Received client UPDATE request for key {} with value {}", id, msg.key(), msg.value());
        
        ArrayList<ActorRef> nodeRefs = new ArrayList<>();
        ArrayList<DataItem> quorum = new ArrayList<>();
        prepareReplicasAndQuorum(msg.key(), nodeRefs, quorum);
        
        int op_id = generateOperationId();
        requestsLedger.put(op_id, new Request(getSender(), RequestType.UPDATE));
        getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, msg.key(), msg.value()));
    }

    private void handleReadDataRequest(ReadDataRequest msg) {
        log.debug("Node[{}]: Handling read data request for key {}", id, msg.key());
        DataItem value = data.get(msg.key());
        getSender().tell(new ReadDataResponse(value), getSelf());
    }

    private void handleOperationResult(OperationResult msg) {
        log.debug("Node[{}]: Received operation result for operation {}", id, msg.op_id());
        Request request = requestsLedger.get(msg.op_id());
        if (request != null) {
            request.getRequester().tell(msg, getSelf());
            requestsLedger.get(msg.op_id()).addResponseResult(msg.result());
        }
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
                .match(ClientGetRequest.class, this::handleClientGetRequest)
                .match(ClientUpdateRequest.class, this::handleClientUpdateRequest)
                .match(ReadDataRequest.class, this::handleReadDataRequest)
                .match(OperationResult.class, this::handleOperationResult)
                .match(SetPeers.class, this::setPeers)
                .match(UpdatePeer.class, this::updatePeer)
                .match(Print.class, this::print)
                .match(PrintPeers.class, this::printPeers)
                .build();
    }
}