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
                        removeNode(scanner, service);
                        break;
                    case "3":
                        updateValue(scanner, service);
                        break;
                    case "4":
                        getValue(scanner, service);
                        break;
                    case "5":
                        printNodeState(scanner, service);
                        break;
                    case "6":
                        printNodePeers(scanner, service);
                        break;
                    case "7":
                        printAllNodes(service);
                        break;
                    case "8":
                        waitForProcessing(scanner, service);
                        break;
                    case "9":
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
        System.out.println("DISTRIBUTED STORAGE SYSTEM - MENU");
        System.out.println("=".repeat(50));
        System.out.println("1. Add Node");
        System.out.println("2. Remove Node");
        System.out.println("3. Update Value (via Client)");
        System.out.println("4. Get Value (via Client)");
        System.out.println("5. Print Node State");
        System.out.println("6. Print Node Peers");
        System.out.println("7. Print All Nodes");
        System.out.println("8. Wait for Processing");
        System.out.println("9. Exit");
        System.out.println("=".repeat(50));
        System.out.print("Enter choice: ");
        System.out.flush();
    }
    
    private static void addNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.addNode(nodeId);
        service.waitForProcessing(1000);
    }
    
    private static void removeNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to remove: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.removeNode(nodeId);
        service.waitForProcessing(500);
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
    
    private static void printNodeState(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.printNode(nodeId);
    }
    
    private static void printNodePeers(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.printPeers(nodeId);
    }
    
    private static void printAllNodes(ManagementService service) {
        System.out.println("\nActive nodes: " + service.getNodes().keySet());
        for (Integer nodeId : service.getNodes().keySet()) {
            service.printNode(nodeId);
        }
    }
    
    private static void waitForProcessing(Scanner scanner, ManagementService service) {
        System.out.print("Enter milliseconds to wait: ");
        long ms = Long.parseLong(scanner.nextLine().trim());
        System.out.println("Waiting " + ms + "ms...");
        service.waitForProcessing(ms);
        System.out.println("Done waiting.");
    }
}
