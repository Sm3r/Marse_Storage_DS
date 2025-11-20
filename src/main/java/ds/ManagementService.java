package ds;

import ds.actors.Node;
import ds.actors.Client;
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


    // Initialize the system with nodes and client
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
        if (!nodes.containsKey(nodeId)) {
            // Pick a bootstrapper node BEFORE creating the new node
            ActorRef bootstrapper = pickRandom(nodes);
            nodes.put(nodeId, system.actorOf(Props.create(Node.class, () -> new Node(nodeId, bootstrapper, delayer)), "node" + nodeId));
            System.out.println("Node " + nodeId + " added. Active nodes: " + nodes.keySet());
        } else {
            System.out.println("Node " + nodeId + " already exists");
        }
    }

    // Remove a node from the system
    public void removeNode(int nodeId) {
        ActorRef removedNode = nodes.remove(nodeId);
        if (removedNode != null) {
            system.stop(removedNode);
            System.out.println("Node " + nodeId + " removed. Active nodes: " + nodes.keySet());
        } else {
            System.out.println("Node " + nodeId + " not found");
        }
    }

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
            System.out.println("Node " + nodeId + " not found");
        }
    }

    // Send print peers message to a node
    public void printPeers(int nodeId) {
        ActorRef node = nodes.get(nodeId);
        if (node != null) {
            delayer.delayedMsg(ActorRef.noSender(), new PrintPeers(), node);
        } else {
            System.out.println("Node " + nodeId + " not found");
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

    // Get all nodes
    public Map<Integer, ActorRef> getNodes() {
        return nodes;
    }

    // Get all clients
    public Map<Integer, ActorRef> getClients() {
        return clients;
    }

    // Get all crashed nodes
    public Map<Integer, ActorRef> getCrashedNodes() {
        return crashedNodes;
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

    // Crash a node
    public void crashNode(int nodeId) {
        ActorRef node = nodes.get(nodeId);
        if (node != null) {
            delayer.delayedMsg(ActorRef.noSender(), new Crash(), node);
            // Move node from active to crashed list
            nodes.remove(nodeId);
            crashedNodes.put(nodeId, node);
            System.out.println("Crash signal sent to node " + nodeId);
        } else {
            System.out.println("Node " + nodeId + " not found");
        }
    }

    // Recover a crashed node
    public void recoverNode(int nodeId, int peerNodeId) {
        ActorRef node = crashedNodes.get(nodeId);
        ActorRef peerNode = nodes.get(peerNodeId);
        
        if (node == null) {
            System.out.println("Node " + nodeId + " not found in crashed nodes");
            return;
        }
        
        if (peerNode == null) {
            System.out.println("Peer node " + peerNodeId + " not found");
            return;
        }
        
        delayer.delayedMsg(ActorRef.noSender(), new Recover(peerNode), node);
        // Move node back from crashed to active list
        crashedNodes.remove(nodeId);
        nodes.put(nodeId, node);
        System.out.println("Recovery signal sent to node " + nodeId + " to contact node " + peerNodeId);
    }

    // Gracefully leave the network
    public void leaveNetwork(int nodeId) {
        ActorRef node = nodes.get(nodeId);
        if (node != null) {
            delayer.delayedMsg(ActorRef.noSender(), new Leave(), node);
            System.out.println("Leave signal sent to node " + nodeId);
            // Remove from our tracking map after sending leave signal
            // The node will stop itself after completing the leave protocol
            nodes.remove(nodeId);
        } else {
            System.out.println("Node " + nodeId + " not found");
        }
    }

    // Terminate the actor system
    public void shutdown() {
        system.terminate();
    }
}
