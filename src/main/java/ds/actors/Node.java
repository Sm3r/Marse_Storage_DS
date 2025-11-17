package ds.actors;

import ds.model.Delayer;
import ds.model.Request;
import ds.model.Request.RequestType;
import ds.model.Types;
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
    private final Delayer delayer;
    private final Map<Integer, DataItem> data;
    private final Map<Integer, ActorRef> peers;
    private final Map<Integer, Request> requestsLedger;

    // Constructors
    public Node(int id, ActorRef bootstrapper, Delayer delayer) {
        this.id = id;
        this.delayer = delayer;
        this.data = new HashMap<>();
        this.peers = new HashMap<>();
        this.requestsLedger = new HashMap<>();

        if (!getSelf().equals(bootstrapper) && bootstrapper != null) {
            // Node is joining an existing network
            log.info("Node[{}]: Starting in joining state, contacting bootstrapper", id);
            getContext().actorSelection(bootstrapper.path()).tell(new JoinRequest(id, getSelf()), getSelf());
        } else {
            // This is the first node
            log.info("Node[{}]: Starting node, transitioning to ready state", id);
            getContext().become(ready());
        }
    }

    // ======================= Helper Methods ====================
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

    private ActorRef getClockwiseNeighbor() {

        List<Integer> allNodeIds = new ArrayList<>(peers.keySet());
        allNodeIds.add(id);
        Collections.sort(allNodeIds);
        
        int n = allNodeIds.size();
        if (n <= 1) {
            return null;
        }
        int currentIndex = allNodeIds.indexOf(id);
        int neighborIndex = (currentIndex + 1) % n;
        int neighborId = allNodeIds.get(neighborIndex);
        
        return peers.get(neighborId);
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

    // ======================= GET/UPDATE operation handlers ====================
    private void handleClientGetRequest(ClientGetRequest msg) {
        log.debug("Node[{}]: Received client GET request for key {}", id, msg.key());
        
        ArrayList<ActorRef> nodeRefs = new ArrayList<>();
        ArrayList<DataItem> quorum = new ArrayList<>();
        prepareReplicasAndQuorum(msg.key(), nodeRefs, quorum);
        
        int op_id = generateOperationId();
        requestsLedger.put(op_id, new Request(getSender(), RequestType.GET, msg.key()));
        getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, msg.key(), delayer));
    }
    
    private void handleClientUpdateRequest(ClientUpdateRequest msg) {
        log.debug("Node[{}]: Received client UPDATE request for key {} with value {}", id, msg.key(), msg.value());
        
        ArrayList<ActorRef> nodeRefs = new ArrayList<>();
        ArrayList<DataItem> quorum = new ArrayList<>();
        prepareReplicasAndQuorum(msg.key(), nodeRefs, quorum);
        
        int op_id = generateOperationId();
        requestsLedger.put(op_id, new Request(getSender(), RequestType.UPDATE, msg.key()));
        getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, msg.key(), msg.value(), delayer));
    }

    private void handleReadDataRequest(ReadDataRequest msg) {
        log.debug("Node[{}]: Handling read data request for key {}", id, msg.key());
        DataItem value = data.get(msg.key());
        getSender().tell(new ReadDataResponse(value), getSelf());
    }

    private void handleWriteDataRequest(WriteDataRequest msg) {
        log.debug("Node[{}]: Updating key {} with value '{}'", id, msg.key(), msg.dataItem().value());
        data.put(msg.key(), msg.dataItem());
    }

    private void handleOperationResult(Result msg) {
        log.debug("Node[{}]: Received operation result for operation {}", id, msg.op_id());
        Request request = requestsLedger.get(msg.op_id());
        if (request != null) {
            delayer.delayedMsg(request.getRequester(), msg, getSelf());
        }
    }

    // ====================== Joining operation handlers ====================

    // === Handle join request from a new node ====
    private void handleJoinRequest(JoinRequest msg) {
        log.info("Node[{}]: Received join request from Node[{}]", id, msg.nodeId());
        if (!peers.containsKey(msg.nodeId())) {
            Map<Integer, ActorRef> nodes = new HashMap<>(peers);
            nodes.put(this.id, getSelf());
            msg.nodeRef().tell(new Types.RegisterPeers(nodes), getSelf());
        } else {
            log.warning("Node[{}]: Node[{}] is already a peer.", id, msg.nodeId());
        }
    }

    private void handleGetAllDataItems(GetAllDataItems msg) {
        log.info("Node[{}]: Received request to send all data items to Node[{}]", id, msg.nodeId());
        Map<Integer, DataItem> dataItems = new HashMap<>();
        for (Map.Entry<Integer, DataItem> entry : data.entrySet()) {
            int key = entry.getKey();
            DataItem value = entry.getValue();
            List<Integer> nodeIds = new ArrayList<>(peers.keySet());
            nodeIds.add(this.id);
            nodeIds.add(msg.nodeId()); // Include the joining node
            List<Integer> replicaIds = findReplicaNodesIds(key, nodeIds);
            if (replicaIds.contains(msg.nodeId())) {
                dataItems.put(key, value);
                log.debug("Node[{}]: Including key {} for Node[{}] (replicas: {})", id, key, msg.nodeId(), replicaIds);
            }
        }
        getSender().tell(new Types.SendAllDataItems(dataItems), getSelf());
    }

    private void handleAddPeer(AddPeer msg) {
        if (!peers.containsKey(msg.id()) && msg.id() != this.id) {
            peers.put(msg.id(), msg.peer());
            log.info("Node[{}]: Added new peer Node[{}]", id, msg.id());
        } else {
            log.warning("Node[{}]: Peer Node[{}] already exists", id, msg.id());
        }
    }


    // === Handle registration from joining node ===
    private void handleRegisterPeers(RegisterPeers msg) {
        this.peers.putAll(msg.peers());
        log.info("Node[{}]: Current peers after registration: {}", id, peers.keySet());

        ActorRef clockwiseNeighbor = getClockwiseNeighbor();
        if (clockwiseNeighbor != null) {
            clockwiseNeighbor.tell(new GetAllDataItems(id), getSelf());
        }
        
        // Transition from joining to ready state after successful peer registration
        log.info("Node[{}]: Join complete, transitioning to ready state", id);
        getContext().become(ready());
    }

    private void handleSendAllDataItems(SendAllDataItems msg) {
        log.info("Node[{}]: Received data items from clockwise neighbor", id);
        this.data.putAll(msg.dataItems());
        
        for (Map.Entry<Integer, DataItem> entry : msg.dataItems().entrySet()) {
            int key = entry.getKey();
            ArrayList<ActorRef> nodeRefs = new ArrayList<>();
            ArrayList<DataItem> quorum = new ArrayList<>();
            prepareReplicasAndQuorum(key, nodeRefs, quorum);
            int op_id = generateOperationId();
            requestsLedger.put(op_id, new Request(getSelf(), RequestType.GET_JOIN, key));
            getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, key, delayer));
            log.debug("Node[{}]: Spawned handler for GET operation on key {} (op_id: {})", id, key, op_id);
        }
    }

    private void handleOperationResultJoin(Result msg) {
        Request request = requestsLedger.get(msg.op_id());
        if (request != null) {
            // Update the value in data if the one in the message is newer
            if (msg.value() != null) {
                DataItem existingData = data.get(request.getDataKey());
                if (existingData == null || msg.value().version() > existingData.version()) {
                    data.put(request.getDataKey(), msg.value());
                    log.info("Node[{}]: Updated key {} with value '{}' (version: {}) from join operation", 
                            id, msg.op_id(), msg.value().value(), msg.value().version());
                }
            }
            
            // Add the result to the ledger
            request.setResult(msg);
            delayer.delayedMsg(request.getRequester(), msg, getSelf());
            
            // Check if all GET_JOIN operations are completed
            boolean allJoinOpsCompleted = true;
            for (Request req : requestsLedger.values()) {
                if (req.getType() == RequestType.GET_JOIN && !req.isCompleted()) {
                    allJoinOpsCompleted = false;
                    break;
                }
            }
            
            // If all GET_JOIN operations are completed, notify all peers to add this node
            if (allJoinOpsCompleted) {
                log.info("Node[{}]: All join operations completed, notifying peers", id);
                for (ActorRef peer : peers.values()) {
                    peer.tell(new AddPeer(id, getSelf()), getSelf());
                }
            }
        }
    }

    

    // ====================== Utility Messages ====================
    private void print(Print msg) {
        log.info("Node[{}]: Data store content: {}", id, data);
    }
    private void printPeers(PrintPeers msg) {
        log.info("Node[{}]: Known peers: {}", id, peers.keySet());
    }


    // ====================== Behavior States ====================
    @Override
    public Receive createReceive() {
        return joining();
    }
    
    private Receive joining() {
        return receiveBuilder()
                .match(RegisterPeers.class, this::handleRegisterPeers)
                .match(SendAllDataItems.class, this::handleSendAllDataItems)
                .match(Result.class, this::handleOperationResultJoin)
                .matchAny(msg -> log.warning("Node[{}]: Rejecting message - node is still joining the network", id))
                .build();
    }
    
    private Receive ready() {
        return receiveBuilder()
                .match(ClientGetRequest.class, this::handleClientGetRequest)
                .match(ClientUpdateRequest.class, this::handleClientUpdateRequest)
                .match(ReadDataRequest.class, this::handleReadDataRequest)
                .match(WriteDataRequest.class, this::handleWriteDataRequest)
                .match(Result.class, this::handleOperationResult)
                .match(JoinRequest.class, this::handleJoinRequest)
                .match(GetAllDataItems.class, this::handleGetAllDataItems)
                .match(AddPeer.class, this::handleAddPeer)
                .match(Print.class, this::print)
                .match(PrintPeers.class, this::printPeers)
                .matchAny(msg -> log.warning("Node[{}]: Received unknown message: {}", id, msg.getClass().getSimpleName()))
                .build();
    }
}