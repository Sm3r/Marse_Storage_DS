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

    // =============== Constructor ====================
    public ManagementService() {
        this.system = ActorSystem.create("MainSystem");
        this.delayer = new Delayer(system);
        this.nodes = new TreeMap<>();
        this.clients = new TreeMap<>();
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
        waitForProcessing(1000);
        addNode(20);
        waitForProcessing(1000);
        addNode(30);
        waitForProcessing(1000);
        addNode(40);
        waitForProcessing(1000);
        addNode(50);

        // Create client actor
        clients.put(1, system.actorOf(Props.create(Client.class, () -> new Client(1, nodes, delayer))));
        clients.put(2, system.actorOf(Props.create(Client.class, () -> new Client(2, nodes, delayer))));
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
        if (node != null) {
            delayer.delayedMsg(node, new Print(), ActorRef.noSender());
        } else {
            System.out.println("Node " + nodeId + " not found");
        }
    }

    // Send print peers message to a node
    public void printPeers(int nodeId) {
        ActorRef node = nodes.get(nodeId);
        if (node != null) {
            delayer.delayedMsg(node, new PrintPeers(), ActorRef.noSender());
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

    // Check if node exists
    public boolean nodeExists(int nodeId) {
        return nodes.containsKey(nodeId);
    }

    // Wait for messages to be processed
    public void waitForProcessing(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Terminate the actor system
    public void shutdown() {
        system.terminate();
    }
}
