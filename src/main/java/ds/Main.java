package ds;

// Main class - minimal entry point
public class Main {

    public static void main(String[] args) {
        
        // Create management service
        ManagementService service = new ManagementService();
        
        System.out.println("=== Starting Execution ===\n");

        System.out.println("- Initializing the system \n");
        // Initialize the system with initial nodes
        service.initialize();
        
        // Wait for initialization messages to be processed
        service.waitForProcessing(2000);

        // Simulation starts here
        System.out.println("\n -Starting simulation\n");
        
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
        
        // Add data to existing nodes
        service.sendUpdateRequest(1,10, 1, "APPLE");
        service.sendUpdateRequest(1,20, 4, "DATE");
        service.sendUpdateRequest(2,30, 7, "GRAPE");
        service.sendUpdateRequest(1,40, 10, "LEMON");
        service.sendUpdateRequest(2,50, 13, "ORANGE");
        
        // Update existing values
        service.sendUpdateRequest(1,10, 2, "BANANA");
        service.sendUpdateRequest(2,20, 5, "ELDERBERRY");
        
        // Add new nodes to the system
        service.addNode(60);
        service.addNode(70);

        // Add more data to new nodes
        service.sendUpdateRequest(2,60, 16, "raspberry");
        service.sendUpdateRequest(2,70, 17, "strawberry");
        
        // Update values in new nodes
        service.sendUpdateRequest(2,60, 16, "RASPBERRY");
        
        // Print current state of all nodes
        service.printNode(10);
        service.printNode(20);
        service.printNode(30);
        service.printNode(40);
        service.printNode(50);
        service.printNode(60);
        service.printNode(70);
        
        // Remove a node
        service.removeNode(30);
        
        // Wait for messages to be processed
        service.waitForProcessing(2000);
        
        // Terminate the ActorSystem to end the program
        service.shutdown();
        System.out.println("\n=== System Terminated ===");
    }
}
