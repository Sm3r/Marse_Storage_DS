package ds;

import ds.actors.Client;
import akka.actor.ActorRef;
import java.util.Scanner;

// Main class - minimal entry point
public class Main {

    public static void main(String[] args) {
        
        // Create management service
        ManagementService service = new ManagementService();
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== Starting Execution ===\n");

        System.out.println("- Initializing the system \n");
        // Initialize the system with initial nodes
        service.initialize();
        
        // Wait for initialization messages to be processed
        service.waitForProcessing(2000);

        System.out.println("\n=== System Initialized ===");
        System.out.println("Active nodes: " + service.getNodes().keySet());
        System.out.println("Active clients: " + service.getClients().keySet());
        
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
        
        // Terminate the ActorSystem to end the program
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
        System.out.flush();
    }
    
    private static void addNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.addNode(nodeId);
        service.waitForProcessing(1000);
    }
    
    private static void updateValue(Scanner scanner, ManagementService service) {
        System.out.print("Enter client ID (1 or 2): ");
        int clientId = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter target node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter key: ");
        int key = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter value: ");
        String value = scanner.nextLine().trim();
        
        ActorRef client = service.getClient(clientId);
        if (client != null) {
            client.tell(new Client.UpdateRequest(nodeId, key, value), ActorRef.noSender());
            System.out.println("UPDATE request sent.");
        } else {
            System.out.println("Client " + clientId + " not found.");
        }
    }
    
    private static void getValue(Scanner scanner, ManagementService service) {
        System.out.print("Enter client ID (1 or 2): ");
        int clientId = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter target node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter key: ");
        int key = Integer.parseInt(scanner.nextLine().trim());
        
        ActorRef client = service.getClient(clientId);
        if (client != null) {
            client.tell(new Client.GetRequest(nodeId, key), ActorRef.noSender());
            System.out.println("GET request sent.");
        } else {
            System.out.println("Client " + clientId + " not found.");
        }
    }
    
    private static void printAllNodes(ManagementService service) {
        service.printNetworkStatus();
        service.waitForProcessing(2000);
    }
    
    private static void crashNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to crash: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.crashNode(nodeId);
        System.out.println("Crash signal sent to node " + nodeId);
    }
    
    private static void recoverNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to recover: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter peer node ID to contact for topology: ");
        int peerNodeId = Integer.parseInt(scanner.nextLine().trim());
        
        service.recoverNode(nodeId, peerNodeId);
        System.out.println("Recovery signal sent to node " + nodeId);
        service.waitForProcessing(1000);
    }
    
    private static void leaveNetwork(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to leave gracefully: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.leaveNetwork(nodeId);
        service.waitForProcessing(3000);
    }
}
