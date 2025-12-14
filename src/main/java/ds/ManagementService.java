package ds;

import ds.actors.Node;
import ds.actors.Client;
import ds.model.Types;
import ds.model.Types.*;
import ds.model.Delayer;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

// Management service for handling all Akka actor operations
public class ManagementService {

    private final ActorSystem system;
    private final Delayer delayer;
    private final Map<Integer, ActorRef> nodes;
    private final Map<Integer, ActorRef> clients;
    private final Map<Integer, ActorRef> crashedNodes;
    
    // Mutual exclusion for operations
    private boolean topologyChangeInProgress = false;
    private int ongoingOperations = 0;

    // =============== Constructor ====================
    public ManagementService() {
        this.system = ActorSystem.create("MainSystem");
        this.delayer = new Delayer(system);
        this.nodes = new TreeMap<>();
        this.clients = new TreeMap<>();
        this.crashedNodes = new TreeMap<>();
    }

    // ================ Utility Functions ====================
    public static ActorRef pickRandom(Map<Integer, ActorRef> map) {
        int size = map.size();
        if (size == 0) return null;

        int targetIndex = ThreadLocalRandom.current().nextInt(size);
        int current = 0;

        for (ActorRef ref : map.values()) {
            if (current == targetIndex) return ref;
            current++;
        }
        return null;
    }
    
    // ================ Operation Tracking ====================
    private synchronized boolean canStartTopologyChange() {
        if (topologyChangeInProgress) {
            System.out.println("✗ ERROR: Another topology change is in progress. Please wait.");
            return false;
        }
        if (ongoingOperations > 0) {
            System.out.println("✗ ERROR: Cannot perform topology change - " + ongoingOperations + " operation(s) in progress. Please wait.");
            return false;
        }
        topologyChangeInProgress = true;
        return true;
    }
    
    private synchronized void finishTopologyChange() {
        topologyChangeInProgress = false;
    }
    
    private synchronized boolean canStartOperation() {
        if (topologyChangeInProgress) {
            System.out.println("✗ ERROR: Topology change in progress. Please wait before starting operations.");
            return false;
        }
        ongoingOperations++;
        return true;
    }
    
    public synchronized void finishOperation() {
        if (ongoingOperations > 0) {
            ongoingOperations--;
        }
    }
    
    public synchronized int getOngoingOperations() {
        return ongoingOperations;
    }
    
    public synchronized boolean isTopologyChangeInProgress() {
        return topologyChangeInProgress;
    }
    
    public boolean startOperation() {
        return canStartOperation();
    }

    // ================ System Initialization ====================
    
    // Initialize the system with nodes and clients
    public void initialize() {

        // Create initial nodes
        addNode(10);
        waitForProcessing(500);
        addNode(20);
        waitForProcessing(500);
        addNode(30);
        waitForProcessing(500);
        addNode(40);
        waitForProcessing(500);
        addNode(50);
        waitForProcessing(500);

        // Create client actor
        clients.put(1, system.actorOf(Props.create(Client.class, () -> new Client(1, nodes, delayer))));
        clients.put(2, system.actorOf(Props.create(Client.class, () -> new Client(2, nodes, delayer))));
        
        // Add some initial data values
        ActorRef client1 = clients.get(1);
        client1.tell(new Client.UpdateRequest(10, 5, "cat"), ActorRef.noSender());
        waitForProcessing(500);
        client1.tell(new Client.UpdateRequest(20, 25, "dog"), ActorRef.noSender());
        waitForProcessing(500);
        client1.tell(new Client.UpdateRequest(30, 45, "frog"), ActorRef.noSender());
        waitForProcessing(500);
    }

    // Add a new node to the system
    public void addNode(int nodeId) {
        if (!canStartTopologyChange()) {
            return;
        }
        
        try {
            if (!nodes.containsKey(nodeId)) {
                ActorRef bootstrapper = pickRandom(nodes);
                nodes.put(nodeId, system.actorOf(Props.create(Node.class, () -> new Node(nodeId, bootstrapper, delayer)), "node" + nodeId));
                System.out.println("✓ Node " + nodeId + " added. Active nodes: " + nodes.keySet());
            } else {
                System.out.println("✗ ERROR: Node " + nodeId + " already exists");
            }
        } finally {
            finishTopologyChange();
        }
    }

    // Remove a node from the system
    public void removeNode(int nodeId) {
        if (!canStartTopologyChange()) {
            return;
        }
        
        try {
            ActorRef removedNode = nodes.remove(nodeId);
            if (removedNode != null) {
                system.stop(removedNode);
                System.out.println("Node " + nodeId + " removed. Active nodes: " + nodes.keySet());
            } else {
                System.out.println("✗ ERROR: Node " + nodeId + " not found");
            }
        } finally {
            finishTopologyChange();
        }
    }

    // ================ Diagnostic and Query Methods ====================
    
    // Send print message to a node
    public void printNode(int nodeId) {
        ActorRef node = nodes.get(nodeId);
        boolean isCrashed = false;
        
        if (node == null) {
            node = crashedNodes.get(nodeId);
            isCrashed = true;
        }
        
        if (node != null) {
            if (isCrashed) {
                System.out.print("X ");
            } else {
                System.out.print("  ");
            }
            delayer.delayedMsg(ActorRef.noSender(), new Print(), node);
        } else {
            System.out.println("✗ ERROR: Node " + nodeId + " not found");
        }
    }

    // Send print peers message to a node
    public void printPeers(int nodeId) {
        ActorRef node = nodes.get(nodeId);
        if (node != null) {
            delayer.delayedMsg(ActorRef.noSender(), new PrintPeers(), node);
        } else {
            System.out.println("✗ ERROR: Node " + nodeId + " not found");
        }
    }

    // Get client actor reference by ID
    public ActorRef getClient(int clientId) {
        return clients.get(clientId);
    }

    // Get node actor reference by ID
    public ActorRef getNode(int nodeId) {
        return nodes.get(nodeId);
    }

    // Check if node exists
    public boolean nodeExists(int nodeId) {
        return nodes.containsKey(nodeId) || crashedNodes.containsKey(nodeId);
    }

    // Wait for messages to be processed
    public void waitForProcessing(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // ================ Node Lifecycle Operations ====================
    
    // Crash a node
    public void crashNode(int nodeId) {
        if (!canStartTopologyChange()) {
            return;
        }
        
        try {
            ActorRef node = nodes.get(nodeId);
            if (node != null) {
                // Check if crashing this node would leave no active nodes
                if (nodes.size() <= 1) {
                    System.out.println("✗ ERROR: Cannot crash node " + nodeId + " - it is the last active node. At least one node must remain active.");
                    return;
                }
                
                delayer.delayedMsg(ActorRef.noSender(), new Crash(), node);
                // Move node from active to crashed list
                nodes.remove(nodeId);
                crashedNodes.put(nodeId, node);
                System.out.println("Crash signal sent to node " + nodeId);
            } else {
                System.out.println("✗ ERROR: Node " + nodeId + " not found");
            }
        } finally {
            finishTopologyChange();
        }
    }

    // Recover a crashed node
    public void recoverNode(int nodeId, int peerNodeId) {
        if (!canStartTopologyChange()) {
            return;
        }
        
        try {
            ActorRef node = crashedNodes.get(nodeId);
            ActorRef peerNode = nodes.get(peerNodeId);
            
            if (node == null) {
                System.out.println("✗ ERROR: Node " + nodeId + " not found in crashed nodes");
                return;
            }
            
            if (peerNode == null) {
                System.out.println("✗ ERROR: Peer node " + peerNodeId + " not found");
                return;
            }
            
            delayer.delayedMsg(ActorRef.noSender(), new Recover(peerNode), node);
            // Move node back from crashed to active list
            crashedNodes.remove(nodeId);
            nodes.put(nodeId, node);
            System.out.println("Recovery signal sent to node " + nodeId + " to contact node " + peerNodeId);
        } finally {
            finishTopologyChange();
        }
    }

    // ================ Client Communication ====================
    
    // Send a request to a client (no delay for user-initiated requests)
    public void sendClientRequest(ActorRef client, Object request) {
        client.tell(request, ActorRef.noSender());
    }
    
    // Collect and print network status from all nodes (active and crashed)
    public void printNetworkStatus() {
        // Create a collector actor to gather responses from all nodes
        ActorRef collector = system.actorOf(Props.create(akka.actor.AbstractActor.class, () -> 
            new akka.actor.AbstractActor() {
                private final java.util.Map<Integer, Types.NetworkStatus> statuses = new java.util.TreeMap<>();
                private final java.util.Set<Integer> activeNodesCollected = new java.util.TreeSet<>();
                private final java.util.Set<Integer> crashedNodesCollected = new java.util.TreeSet<>();
                private int expectedResponses = 0;
                private int receivedResponses = 0;
                
                @Override
                public void preStart() {
                    expectedResponses = nodes.size() + ManagementService.this.crashedNodes.size();
                }
                
                @Override
                public Receive createReceive() {
                    return receiveBuilder()
                        .match(Types.NetworkStatus.class, msg -> {
                            statuses.put(msg.nodeId(), msg);
                            if (msg.isCrashed()) {
                                crashedNodesCollected.add(msg.nodeId());
                            } else {
                                activeNodesCollected.add(msg.nodeId());
                            }
                            receivedResponses++;
                            
                            if (receivedResponses >= expectedResponses) {
                                // Print the collected information
                                System.out.println("\n=== Network Status ===");
                                System.out.println("\nClients: " + clients.keySet());
                                System.out.println("Active nodes: " + activeNodesCollected);
                                System.out.println("Crashed nodes: " + crashedNodesCollected);
                                System.out.println("\nNode States:");
                                
                                for (Integer nodeId : statuses.keySet()) {
                                    printNode(nodeId);
                                    waitForProcessing(150);
                                }
                                
                                getContext().stop(getSelf());
                            }
                        })
                        .build();
                }
            }
        ));
        
        // Request status from all nodes
        for (ActorRef node : nodes.values()) {
            delayer.delayedMsg(collector, new Types.PrintNetwork(), node);
        }
        for (ActorRef node : ManagementService.this.crashedNodes.values()) {
            delayer.delayedMsg(collector, new Types.PrintNetwork(), node);
        }
    }

    // Gracefully leave the network
    public void leaveNetwork(int nodeId) {
        if (!canStartTopologyChange()) {
            return;
        }
        
        try {
            ActorRef node = nodes.get(nodeId);
            if (node != null) {
                // Check if leaving this node would violate the N constraint
                // Consider total nodes (active + crashed) to allow leaving even with crashed nodes
                int totalNodesAfterLeave = (nodes.size() - 1) + crashedNodes.size();
                if (totalNodesAfterLeave < ds.config.Settings.N) {
                    System.out.println("✗ ERROR: Cannot leave network - node " + nodeId + " leaving would result in " + totalNodesAfterLeave + " total nodes, but N=" + ds.config.Settings.N + " requires at least " + ds.config.Settings.N + " nodes in the system.");
                    return;
                }
                
                delayer.delayedMsg(ActorRef.noSender(), new Leave(), node);
                System.out.println("Leave signal sent to node " + nodeId + " (Note: Some operations may timeout if crashed nodes are part of quorums)");
                // Remove from our tracking map after sending leave signal
                // The node will stop itself after completing the leave protocol
                nodes.remove(nodeId);
            } else {
                System.out.println("✗ ERROR: Node " + nodeId + " not found");
            }
        } finally {
            finishTopologyChange();
        }
    }

    // Terminate the actor system
    public void shutdown() {
        system.terminate();
    }
}
