package ds;

import ds.actors.Client;
import ds.config.Settings;
import akka.actor.ActorRef;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        
        Scanner scanner = new Scanner(System.in);
        Settings.configure(scanner);
        
        ManagementService service = new ManagementService();
        scanner = new Scanner(System.in);
        
        System.out.println("=== Starting Execution ===\n");
        service.initialize();
        service.waitForProcessing(2000);
        
        // Interactive TUI
        boolean running = true;
        while (running) {
            displayMenu();
            
            try {
                String choice = scanner.nextLine().trim();
                
                switch (choice) {
                    case "1":
                        addNode(scanner, service);
                        break;
                    case "2":
                        updateValue(scanner, service);
                        break;
                    case "3":
                        getValue(scanner, service);
                        break;
                    case "4":
                        printAllNodes(service);
                        break;
                    case "5":
                        crashNode(scanner, service);
                        break;
                    case "6":
                        recoverNode(scanner, service);
                        break;
                    case "7":
                        leaveNetwork(scanner, service);
                        break;
                    case "8":
                        running = false;
                        System.out.println("\nShutting down...");
                        break;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
        scanner.close();
        
        service.shutdown();
        System.out.println("\n=== System Terminated ===");
    }
    
    private static void displayMenu() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("DISTRIBUTED STORAGE SYSTEM");
        System.out.println("=".repeat(50));
        System.out.println("1. Add Node");
        System.out.println("2. Update Value");
        System.out.println("3. Get Value");
        System.out.println("4. Print Network");
        System.out.println("5. Crash Node");
        System.out.println("6. Recover Node");
        System.out.println("7. Leave Network");
        System.out.println("8. Exit");
        System.out.println("=".repeat(50));
        System.out.print("> ");
        System.out.flush();
    }
    
    private static void addNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.addNode(nodeId);
        service.waitForProcessing(1000);
    }
    
    private static void updateValue(Scanner scanner, ManagementService service) {
        if (!service.startOperation()) {
            return;
        }
        
        try {
            System.out.print("Enter client ID: ");
            int clientId = Integer.parseInt(scanner.nextLine().trim());
            
            ActorRef client = service.getClient(clientId);
            if (client == null) {
                System.out.println("✗ ERROR: Client " + clientId + " not found.");
                return;
            }
            
            System.out.print("Enter target node ID: ");
            int nodeId = Integer.parseInt(scanner.nextLine().trim());
            
            if (!service.nodeExists(nodeId)) {
                System.out.println("✗ ERROR: Node " + nodeId + " is not present in the system.");
                return;
            }
            
            System.out.print("Enter key: ");
            int key = Integer.parseInt(scanner.nextLine().trim());
            
            System.out.print("Enter value: ");
            String value = scanner.nextLine().trim();
            
            client.tell(new Client.UpdateRequest(nodeId, key, value), ActorRef.noSender());
            System.out.println("✓ UPDATE request sent.");
            
            // Wait for operation to complete
            service.waitForProcessing(1000);
        } finally {
            service.finishOperation();
        }
    }
    
    private static void getValue(Scanner scanner, ManagementService service) {
        if (!service.startOperation()) {
            return;
        }
        
        try {
            System.out.print("Enter client ID (1 or 2): ");
            int clientId = Integer.parseInt(scanner.nextLine().trim());
            
            ActorRef client = service.getClient(clientId);
            if (client == null) {
                System.out.println("✗ ERROR: Client " + clientId + " not found.");
                return;
            }
            
            System.out.print("Enter target node ID: ");
            int nodeId = Integer.parseInt(scanner.nextLine().trim());
            
            if (!service.nodeExists(nodeId)) {
                System.out.println("✗ ERROR: Node " + nodeId + " is not present in the system.");
                return;
            }
            
            System.out.print("Enter key: ");
            int key = Integer.parseInt(scanner.nextLine().trim());
            
            client.tell(new Client.GetRequest(nodeId, key), ActorRef.noSender());
            System.out.println("✓ GET request sent.");
            
            // Wait for operation to complete
            service.waitForProcessing(1000);
        } finally {
            service.finishOperation();
        }
    }
    
    private static void crashNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to crash: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        if (!service.nodeExists(nodeId)) {
            System.out.println("✗ ERROR: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        service.crashNode(nodeId);
    }
    
    private static void recoverNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to recover: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());

        if (!service.nodeExists(nodeId)) {
            System.out.println("✗ ERROR: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        System.out.print("Enter peer node ID to contact for topology: ");
        int peerNodeId = Integer.parseInt(scanner.nextLine().trim());

        if (!service.nodeExists(peerNodeId)) {
            System.out.println("✗ ERROR: Peer node " + peerNodeId + " is not present in the system.");
            return;
        }
        
        service.recoverNode(nodeId, peerNodeId);
        service.waitForProcessing(1000);
    }
    
    private static void leaveNetwork(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to leave gracefully: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        if (!service.nodeExists(nodeId)) {
            System.out.println("✗ ERROR: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        service.leaveNetwork(nodeId);
        service.waitForProcessing(3000);
    }

    private static void printAllNodes(ManagementService service) {
        service.printNetworkStatus();
        service.waitForProcessing(2000);
    }
}
