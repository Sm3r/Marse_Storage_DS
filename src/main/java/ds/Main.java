package ds;
import ds.actors.Node;
import ds.actors.Client;
import ds.data_structures.DataItem;
import ds.data_structures.Messages.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.Map;
import java.util.TreeMap;

// Main class
public class Main {

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("MainSystem");

        // Prepare initial data for each node
        Map<Integer, DataItem> dataNode10 = new TreeMap<>();
        dataNode10.put(1, new DataItem("apple"));
        dataNode10.put(2, new DataItem("banana"));
        
        Map<Integer, DataItem> dataNode20 = new TreeMap<>();
        dataNode20.put(3, new DataItem("cherry"));
        dataNode20.put(4, new DataItem("date"));
        
        Map<Integer, DataItem> dataNode30 = new TreeMap<>();
        dataNode30.put(5, new DataItem("elderberry"));
        dataNode30.put(6, new DataItem("fig"));

        // Create node actors with initial data (peers will be set after creation)
        Map<Integer, ActorRef> nodes = new TreeMap<>();
        nodes.put(10, system.actorOf(Props.create(Node.class, () -> new Node(10, new java.util.HashMap<>(), dataNode10))));
        nodes.put(20, system.actorOf(Props.create(Node.class, () -> new Node(20, new java.util.HashMap<>(), dataNode20))));
        nodes.put(30, system.actorOf(Props.create(Node.class, () -> new Node(30, new java.util.HashMap<>(), dataNode30))));

        // Create client actor
        ActorRef client = system.actorOf(Props.create(Client.class, 1, nodes));

        // Set peers for each node (each node knows about all other nodes)
        for (Map.Entry<Integer, ActorRef> entry : nodes.entrySet()) {
            int nodeId = entry.getKey();
            
            // Create a map of peers excluding the node itself
            Map<Integer, ActorRef> peersForNode = new TreeMap<>();
            for (Map.Entry<Integer, ActorRef> peerEntry : nodes.entrySet()) {
                if (peerEntry.getKey() != nodeId) {
                    peersForNode.put(peerEntry.getKey(), peerEntry.getValue());
                }
            }
            
            client.tell(new Client.NodeMessage(nodeId, new Node.SetPeers(peersForNode)), 
                    ActorRef.noSender());
        }

        // Complex execution scenario
        System.out.println("=== Starting Complex Execution ===\n");
        
        // Print initial state of all nodes (with initial data)
        System.out.println("--- Initial State ---");
        client.tell(new Client.NodeMessage(10, new Node.Print()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(20, new Node.Print()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(30, new Node.Print()),
                ActorRef.noSender());
        
        // Print peers for each node
        client.tell(new Client.NodeMessage(10, new Node.PrintPeers()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(20, new Node.PrintPeers()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(30, new Node.PrintPeers()),
                ActorRef.noSender());
        
        // Add new nodes dynamically
        System.out.println("\n--- Adding New Nodes ---");
        client.tell(new Client.AddNode(40), ActorRef.noSender());
        client.tell(new Client.AddNode(50), ActorRef.noSender());
        
        // Add data operations to existing nodes
        System.out.println("\n--- Data Operations on Existing Nodes ---");
        client.tell(new Client.NodeMessage(10, new Node.Update(1, "one")),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(10, new Node.Update(2, "two")),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(20, new Node.Update(1, "uno")),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(20, new Node.Update(3, "three")),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(30, new Node.Update(4, "four")),
                ActorRef.noSender());
        
        // Update existing values
        System.out.println("\n--- Updating Existing Values ---");
        client.tell(new Client.NodeMessage(10, new Node.Update(1, "ONE")),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(20, new Node.Update(1, "UNO")),
                ActorRef.noSender());
        
        // Add more data to new nodes
        System.out.println("\n--- Operations on New Nodes ---");
        client.tell(new Client.NodeMessage(40, new Node.Update(5, "five")),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(50, new Node.Update(6, "six")),
                ActorRef.noSender());
        
        // Update values in new nodes
        client.tell(new Client.NodeMessage(40, new Node.Update(5, "FIVE")),
                ActorRef.noSender());
        
        // Print current state of all nodes
        System.out.println("\n--- Final State of All Nodes ---");
        client.tell(new Client.NodeMessage(10, new Node.Print()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(20, new Node.Print()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(30, new Node.Print()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(40, new Node.Print()),
                ActorRef.noSender());
        client.tell(new Client.NodeMessage(50, new Node.Print()),
                ActorRef.noSender());
        
        // Remove a node
        System.out.println("\n--- Node Removal Test ---");
        client.tell(new Client.RemoveNode(30), ActorRef.noSender());
        
        // Print active nodes
        client.tell(new Client.PrintNodes(), ActorRef.noSender());
        
        // Try to send message to removed node (should show warning)
        client.tell(new Client.NodeMessage(30, new Node.Print()),
                ActorRef.noSender());
        
        System.out.println("\n=== Execution Commands Sent ===");
        
        // Wait for messages to be processed
        try {
            Thread.sleep(2000); // Wait 2 seconds for all messages to be processed
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        // Terminate the ActorSystem to end the program
        system.terminate();
        System.out.println("\n=== System Terminated ===");
    }
}
