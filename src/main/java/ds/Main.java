package ds;

// Main class - minimal entry point
public class Main {

    public static void main(String[] args) {
        
        // Create management service
        ManagementService service = new ManagementService();
        
        // Initialize the system with initial nodes
        service.initialize();
        
        // Execution
        System.out.println("=== Starting Execution ===\n");
        
        // Print initial state
        service.printNode(10);
        service.printNode(20);
        service.printNode(30);
        service.printNode(40);
        service.printNode(50);
        
        // Print peers for each node
        service.printPeers(10);
        service.printPeers(20);
        service.printPeers(30);
        service.printPeers(40);
        service.printPeers(50);
        
        // Add data operations to existing nodes
        System.out.println("\n--- Data Operations on Existing Nodes ---");
        service.sendUpdateRequest(1,10, 1, "APPLE");
        service.sendUpdateRequest(1,20, 4, "DATE");
        service.sendUpdateRequest(2,30, 7, "GRAPE");
        service.sendUpdateRequest(1,40, 10, "LEMON");
        service.sendUpdateRequest(2,50, 13, "ORANGE");
        
        // Update existing values
        System.out.println("\n--- Updating Existing Values ---");
        service.sendUpdateRequest(1,10, 2, "BANANA");
        service.sendUpdateRequest(2,20, 5, "ELDERBERRY");
        
        // Add more data to new nodes
        System.out.println("\n--- Operations on New Nodes ---");
        service.sendUpdateRequest(2,60, 16, "raspberry");
        service.sendUpdateRequest(2,70, 17, "strawberry");
        
        // Update values in new nodes
        service.sendUpdateRequest(2,60, 16, "RASPBERRY");
        
        // Print current state of all nodes
        System.out.println("\n--- Final State of All Nodes ---");
        service.printNode(10);
        service.printNode(20);
        service.printNode(30);
        service.printNode(40);
        service.printNode(50);
        service.printNode(60);
        service.printNode(70);
        
        // Remove a node
        System.out.println("\n--- Node Removal Test ---");
        service.removeNode(30);
        
        // Try to send message to removed node
        if (!service.nodeExists(30)) {
            System.out.println("Node 30 is no longer available");
        }
        
        System.out.println("\n=== Execution Commands Sent ===");
        
        // Wait for messages to be processed
        service.waitForProcessing(2000);
        
        // Terminate the ActorSystem to end the program
        service.shutdown();
        System.out.println("\n=== System Terminated ===");
    }
}
