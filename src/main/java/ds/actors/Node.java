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
import akka.actor.Cancellable;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// Node actor
public class Node extends AbstractActor {

    // Node fields
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private final Delayer delayer;
    private final Map<Integer, DataItem> data;
    private final Map<Integer, ActorRef> peers;
    private final Map<Integer, Request> requestsLedger;
    private int responseReceived = 0;
    private Cancellable leaveTimeout = null;

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
        List<ActorRef> neighbors = getClockwiseNeighbors(1);
        return neighbors.isEmpty() ? null : neighbors.get(0);
    }

    private List<ActorRef> getClockwiseNeighbors(Integer n) {
        List<ActorRef> neighbors = new ArrayList<>();
        List<Integer> allNodeIds = new ArrayList<>(peers.keySet());
        allNodeIds.add(id);
        Collections.sort(allNodeIds);
        
        int totalNodes = allNodeIds.size();
        if (totalNodes <= 1) {
            return neighbors;
        }
        
        // If n is null, use Settings.N
        int count = (n != null) ? n : Settings.N;
        
        // Limit count to available nodes (excluding self)
        count = Math.min(count, totalNodes - 1);
        
        int currentIndex = allNodeIds.indexOf(id);
        
        for (int i = 1; i <= count; i++) {
            int neighborIndex = (currentIndex + i) % totalNodes;
            int neighborId = allNodeIds.get(neighborIndex);
            ActorRef neighborRef = peers.get(neighborId);
            if (neighborRef != null) {
                neighbors.add(neighborRef);
            }
        }
        
        return neighbors;
    }

    private boolean prepareReplicasAndQuorum(int key, ArrayList<ActorRef> nodeRefs, ArrayList<DataItem> quorum) {
        List<Integer> nodeIds = new ArrayList<>(peers.keySet());
        nodeIds.add(id);
        List<Integer> replicaIds = findReplicaNodesIds(key, nodeIds);
        boolean coordinatorIsReplica = false;
        
        for (Integer nodeId : replicaIds) {
            if (nodeId == id) {
                coordinatorIsReplica = true;
                DataItem localData = data.get(key);
                if (localData != null) {
                    quorum.add(localData);
                }
            } else {
                nodeRefs.add(peers.get(nodeId));
            }
        }
        return coordinatorIsReplica;
    }

    private int generateOperationId() {
        return requestsLedger.size() + 1;
    }

    // ======================= GET/UPDATE operation handlers ====================
    private void handleClientGetRequest(ClientGetRequest msg) {
        log.debug("Node[{}]: Received client GET request for key {}", id, msg.key());
        
        ArrayList<ActorRef> nodeRefs = new ArrayList<>();
        ArrayList<DataItem> quorum = new ArrayList<>();
        boolean coordinatorIsReplica = prepareReplicasAndQuorum(msg.key(), nodeRefs, quorum);
        
        int op_id = generateOperationId();
        requestsLedger.put(op_id, new Request(getSender(), RequestType.GET, msg.key()));
        getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, msg.key(), coordinatorIsReplica, delayer));
    }
    
    private void handleClientUpdateRequest(ClientUpdateRequest msg) {
        log.info("Node[{}]: Received client UPDATE request for key {} with value {}", id, msg.key(), msg.value());
        
        ArrayList<ActorRef> nodeRefs = new ArrayList<>();
        ArrayList<DataItem> quorum = new ArrayList<>();
        boolean coordinatorIsReplica = prepareReplicasAndQuorum(msg.key(), nodeRefs, quorum);
        
        int op_id = generateOperationId();
        requestsLedger.put(op_id, new Request(getSender(), RequestType.UPDATE, msg.key()));
        getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, msg.key(), msg.value(), coordinatorIsReplica, delayer));
    }

    private void handleReadDataRequest(ReadDataRequest msg) {
        log.info("Node[{}]: Handling read data request for key {}", id, msg.key());
        DataItem value = data.get(msg.key());
        getSender().tell(new ReadDataResponse(value), getSelf());
    }

    private void handleWriteDataRequest(WriteDataRequest msg) {
        log.info("Node[{}]: Updating key {} with value '{}'", id, msg.key(), msg.dataItem().value());
        data.put(msg.key(), msg.dataItem());
    }

    private void handleOperationResult(Result msg) {
        log.debug("Node[{}]: Received operation result for operation {}", id, msg.op_id());
        Request request = requestsLedger.get(msg.op_id());
        if (request != null) {
            delayer.delayedMsg(request.getRequester(), msg, getSelf());
        }
    }

    // ====================== Crash/Recover operation handlers ====================
    private void handleCrash(Crash msg) {
        log.warning("Node[{}]: Crashing as per request", id);
        getContext().become(crashed());
    }

    private void handleRecover(Recover msg) {
        log.info("Node[{}]: Recovering from crash", id);
        delayer.delayedMsg(msg.nodeRef(), new Types.TopologyRequest(), getSelf());
    }

    private void handleTopologyRequest(TopologyRequest msg) {
        log.info("Node[{}]: Received topology request, sending topology response", id);
        HashMap<Integer, ActorRef> topology = new HashMap<>(this.peers);
        topology.put(this.id, getSelf());
        getSender().tell(new TopologyResponse(new HashMap<>(topology)), getSelf());
    }

    private void handleTopologyResponse(TopologyResponse msg) {
        this.peers.putAll(msg.peers());
        
        log.info("Node[{}]: Received topology with {} peers, updating data responsibilities", id, peers.size());
        
        List<Integer> keysToRemove = new ArrayList<>();
        for (Map.Entry<Integer, DataItem> entry : data.entrySet()) {
            int key = entry.getKey();
            List<Integer> nodeIds = new ArrayList<>(peers.keySet());
            nodeIds.add(this.id);
            List<Integer> replicaIds = findReplicaNodesIds(key, nodeIds);
            
            if (!replicaIds.contains(this.id)) {
                keysToRemove.add(key);
                log.info("Node[{}]: Dropping key {} after recovery (no longer responsible, new replicas: {})", id, key, replicaIds);
            }
        }
        for (Integer key : keysToRemove) {
            data.remove(key);
        }
        getContext().become(ready());
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
            
            // Check if we need to drop any data we're no longer responsible for
            List<Integer> keysToRemove = new ArrayList<>();
            for (Map.Entry<Integer, DataItem> entry : data.entrySet()) {
                int key = entry.getKey();
                List<Integer> nodeIds = new ArrayList<>(peers.keySet());
                nodeIds.add(this.id);
                List<Integer> replicaIds = findReplicaNodesIds(key, nodeIds);
                
                // If this node is no longer in the replica set for this key, remove it
                if (!replicaIds.contains(this.id)) {
                    keysToRemove.add(key);
                    log.info("Node[{}]: Dropping key {} (no longer responsible, new replicas: {})", id, key, replicaIds);
                }
            }
            
            // Remove the keys we're no longer responsible for
            for (Integer key : keysToRemove) {
                data.remove(key);
            }
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
            log.info("Node[{}]: Waiting for data items from clockwise neighbor before transitioning to ready state", id);
        } else {
            // No clockwise neighbor, so no data to receive - transition immediately
            log.info("Node[{}]: No clockwise neighbor found, transitioning to ready state", id);
            getContext().become(ready());
        }
    }

    private void handleSendAllDataItems(SendAllDataItems msg) {
        log.info("Node[{}]: Received {} data items from clockwise neighbor", id, msg.dataItems().size());
        this.data.putAll(msg.dataItems());
        
        if (msg.dataItems().isEmpty()) {
            // No data items to sync, transition to ready state immediately
            log.info("Node[{}]: No data items to sync, transitioning to ready state", id);
            getContext().become(ready());
            
            // Notify all peers to add this node
            for (ActorRef peer : peers.values()) {
                peer.tell(new AddPeer(id, getSelf()), getSelf());
            }
        } else {
            // Spawn handlers to sync data items
            for (Map.Entry<Integer, DataItem> entry : msg.dataItems().entrySet()) {
                int key = entry.getKey();
                ArrayList<ActorRef> nodeRefs = new ArrayList<>();
                ArrayList<DataItem> quorum = new ArrayList<>();
                boolean coordinatorIsReplica = prepareReplicasAndQuorum(key, nodeRefs, quorum);
                int op_id = generateOperationId();
                requestsLedger.put(op_id, new Request(getSelf(), RequestType.GET_JOIN, key));
                getContext().actorOf(Props.create(Handler.class, op_id, getSelf(), nodeRefs, quorum, key, coordinatorIsReplica, delayer));
                log.debug("Node[{}]: Spawned handler for GET operation on key {} (op_id: {})", id, key, op_id);
            }
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
                getContext().become(ready());
            }
        }
    }

    // ======================= Leaving operation handlers ====================
    private void handleLeave(Leave msg) {
        log.debug("Node[{}]: Received leave request, notifying peers", id);
        List<ActorRef> clockwiseNeighbors = getClockwiseNeighbors(Settings.N);
        for (ActorRef neighbor : clockwiseNeighbors) {
            delayer.delayedMsg(neighbor, new AckRequest(), getSelf());
        }
        
        // Schedule timeout for leave operation (2000ms)
        leaveTimeout = getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(2000, TimeUnit.MILLISECONDS),
            getSelf(),
            new OperationTimeout(),
            getContext().getSystem().dispatcher(),
            getSelf()
        );
        log.debug("Node[{}]: Leave timeout scheduled for 2000ms", id);
    }

    private void handleAckRequest(AckRequest msg) {
        log.debug("Node[{}]: Received AckRequest, sending AckResponse", id);
        delayer.delayedMsg(getSender(), new AckResponse(id), getSelf());
    }

    private void handleAckResponse(AckResponse msg) {
        responseReceived++;
        log.debug("Node[{}]: Received AckResponse from Node[{}] (total acks: {})", id, msg.nodeId(), responseReceived);
        if (responseReceived == Settings.N) {
            // Cancel the timeout since we received all acks
            if (leaveTimeout != null && !leaveTimeout.isCancelled()) {
                leaveTimeout.cancel();
                log.debug("Node[{}]: Leave timeout cancelled - all acks received", id);
            }
            
            List<ActorRef> clockwiseNeighbors = getClockwiseNeighbors(Settings.N);
            for (ActorRef neighbor : clockwiseNeighbors) {
                for (Map.Entry<Integer, DataItem> entry : data.entrySet()) {
                    int key = entry.getKey();
                    DataItem value = entry.getValue();
                    List<Integer> nodeIds = new ArrayList<>(peers.keySet());
                    nodeIds.add(this.id);
                    List<Integer> replicaIds = findReplicaNodesIds(key, nodeIds);
                    if (replicaIds.contains(msg.nodeId())) {
                        delayer.delayedMsg(neighbor, new WriteDataRequest(key, value), getSelf());
                        log.debug("Node[{}]: Sending key {} to Node[{}] before leaving (replicas: {})", id, key, msg.nodeId(), replicaIds);
                    }
                }
            }
            for (ActorRef peer : peers.values()) {
                delayer.delayedMsg(peer, new LeaveNotify(id), getSelf());
            }
            log.info("Node[{}]: Received all AckResponses, leaving the network", id);
            getContext().stop(getSelf());
        }
    }

    private void handleLeaveNotify(LeaveNotify msg) {
        log.info("Node[{}]: Received leave notification from Node[{}], removing from peers", id, msg.nodeId());
        peers.remove(msg.nodeId());
    }

    private void handleOperationTimeout(OperationTimeout msg) {
        // Check if this timeout is for a leave operation
        if (leaveTimeout != null) {
            log.warning("Node[{}]: Leave operation timeout - only received {} of {} required acks, aborting leave", 
                        id, responseReceived, Settings.N);
            // Reset state and abort the leave operation
            responseReceived = 0;
            leaveTimeout = null;
        }
    }
    

    // ====================== Utility Messages ====================
    private String formatDataStore() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Integer, DataItem> entry : data.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(entry.getKey())
              .append("=[")
              .append(entry.getValue().value())
              .append(", v=")
              .append(entry.getValue().version())
              .append("]");
        }
        sb.append("}");
        return sb.toString();
    }
    
    private void print(Print msg) {
        String output = String.format("Node[%d]: Data: %s", id, formatDataStore());
        log.info(output);
        System.out.println(output);
    }
    private void printPeers(PrintPeers msg) {
        String output = String.format("Node[%d]: Known peers: %s", id, peers.keySet());
        log.info(output);
        System.out.println(output);
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
                // GET/UPDATE operation handlers
                .match(ClientGetRequest.class, this::handleClientGetRequest)
                .match(ClientUpdateRequest.class, this::handleClientUpdateRequest)
                .match(ReadDataRequest.class, this::handleReadDataRequest)
                .match(WriteDataRequest.class, this::handleWriteDataRequest)
                .match(Result.class, this::handleOperationResult)
                // Crash/Recover handlers
                .match(Crash.class, this::handleCrash)
                .match(TopologyRequest.class, this::handleTopologyRequest)
                // Joining operation handlers
                .match(JoinRequest.class, this::handleJoinRequest)
                .match(GetAllDataItems.class, this::handleGetAllDataItems)
                .match(AddPeer.class, this::handleAddPeer)
                // Leaving operation handlers
                .match(Leave.class, this::handleLeave)
                .match(AckRequest.class, this::handleAckRequest)
                .match(AckResponse.class, this::handleAckResponse)
                .match(LeaveNotify.class, this::handleLeaveNotify)
                .match(OperationTimeout.class, this::handleOperationTimeout)
                // Utility messages
                .match(Print.class, this::print)
                .match(PrintPeers.class, this::printPeers)
                .matchAny(msg -> log.warning("Node[{}]: Received unknown message: {}", id, msg.getClass().getSimpleName()))
                .build();
    }

    private Receive crashed() {
        return receiveBuilder()
                .match(Recover.class, this::handleRecover)
                .match(TopologyResponse.class, this::handleTopologyResponse)
                .match(Print.class, this::print)
                .matchAny(msg -> log.warning("Node[{}]: Node is crashed. Ignoring message: {}", id, msg.getClass().getSimpleName()))
                .build();
    }
}