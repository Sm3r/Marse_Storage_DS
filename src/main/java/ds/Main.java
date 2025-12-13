package ds;

import ds.actors.Client;
import ds.config.Settings;
import akka.actor.ActorRef;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        
        Scanner scanner = new Scanner(System.in);
        configureSystemParameters(scanner);
        
        ManagementService service = new ManagementService();
        scanner = new Scanner(System.in);
        
        System.out.println("=== Starting Execution ===\n");

        System.out.println("- Initializing the system \n");
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
        System.out.flush();
    }
    
    private static void addNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        service.addNode(nodeId);
        service.waitForProcessing(1000);
    }
    
    private static void updateValue(Scanner scanner, ManagementService service) {
        System.out.print("Enter client ID: ");
        int clientId = Integer.parseInt(scanner.nextLine().trim());
        
        ActorRef client = service.getClient(clientId);
        if (client == null) {
            System.out.println("Error: Client " + clientId + " not found.");
            return;
        }
        
        System.out.print("Enter target node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        if (!service.nodeExists(nodeId)) {
            System.out.println("Error: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        System.out.print("Enter key: ");
        int key = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter value: ");
        String value = scanner.nextLine().trim();
        
        client.tell(new Client.UpdateRequest(nodeId, key, value), ActorRef.noSender());
        System.out.println("UPDATE request sent.");
    }
    
    private static void getValue(Scanner scanner, ManagementService service) {
        System.out.print("Enter client ID (1 or 2): ");
        int clientId = Integer.parseInt(scanner.nextLine().trim());
        
        ActorRef client = service.getClient(clientId);
        if (client == null) {
            System.out.println("Error: Client " + clientId + " not found.");
            return;
        }
        
        System.out.print("Enter target node ID: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        if (!service.nodeExists(nodeId)) {
            System.out.println("Error: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        System.out.print("Enter key: ");
        int key = Integer.parseInt(scanner.nextLine().trim());
        
        client.tell(new Client.GetRequest(nodeId, key), ActorRef.noSender());
        System.out.println("GET request sent.");
    }
    
    private static void crashNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to crash: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        if (!service.nodeExists(nodeId)) {
            System.out.println("Error: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        service.crashNode(nodeId);
        System.out.println("Crash signal sent to node " + nodeId);
    }
    
    private static void recoverNode(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to recover: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());

        if (!service.nodeExists(nodeId)) {
            System.out.println("Error: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        System.out.print("Enter peer node ID to contact for topology: ");
        int peerNodeId = Integer.parseInt(scanner.nextLine().trim());

        if (!service.nodeExists(peerNodeId)) {
            System.out.println("Error: Peer node " + peerNodeId + " is not present in the system.");
            return;
        }
        
        service.recoverNode(nodeId, peerNodeId);
        System.out.println("Recovery signal sent to node " + nodeId);
        service.waitForProcessing(1000);
    }
    
    private static void leaveNetwork(Scanner scanner, ManagementService service) {
        System.out.print("Enter node ID to leave gracefully: ");
        int nodeId = Integer.parseInt(scanner.nextLine().trim());
        
        if (!service.nodeExists(nodeId)) {
            System.out.println("Error: Node " + nodeId + " is not present in the system.");
            return;
        }
        
        service.leaveNetwork(nodeId);
        service.waitForProcessing(3000);
    }

    private static void printAllNodes(ManagementService service) {
        service.printNetworkStatus();
        service.waitForProcessing(2000);
    }
    
    // ==================== CONFIGURATION ====================
    private static void configureSystemParameters(Scanner scanner) {
        System.out.println("\n" + "=".repeat(15));
        System.out.println("Configuration");
        System.out.println("=".repeat(15));
        System.out.println();
        System.out.println("Please configure system parameters.");
        System.out.println("Press ENTER to use default values shown in [brackets]\n");
        
        // Default values
        final int DEFAULT_N = 3;
        final int DEFAULT_R = 2;
        final int DEFAULT_W = 2;
        final int DEFAULT_T = 1000;
        
        boolean validConfig = false;
        
        while (!validConfig) {
            // Use temporary variables to avoid polluting Settings with invalid values
            int tempN = DEFAULT_N;
            int tempR = DEFAULT_R;
            int tempW = DEFAULT_W;
            int tempT = DEFAULT_T;
            
            try {
                System.out.print("Replication Factor (N) [" + DEFAULT_N + "]: ");
                String nInput = scanner.nextLine().trim();
                if (!nInput.isEmpty()) {
                    tempN = Integer.parseInt(nInput);
                }
                System.out.print("Read Quorum (R) [" + DEFAULT_R + "]: ");
                String rInput = scanner.nextLine().trim();
                if (!rInput.isEmpty()) {
                    tempR = Integer.parseInt(rInput);
                }
                System.out.print("Write Quorum (W) [" + DEFAULT_W + "]: ");
                String wInput = scanner.nextLine().trim();
                if (!wInput.isEmpty()) {
                    tempW = Integer.parseInt(wInput);
                }
                System.out.print("Timeout in milliseconds (T) [" + DEFAULT_T + "]: ");
                String tInput = scanner.nextLine().trim();
                if (!tInput.isEmpty()) {
                    tempT = Integer.parseInt(tInput);
                }
                System.out.println();

                Settings.N = tempN;
                Settings.R = tempR;
                Settings.W = tempW;
                Settings.T = tempT;
                
                // Validate configuration
                if (Settings.validateConstraints()) {
                    validConfig = true;
                    Settings.printConfiguration();
                }
                
            } catch (NumberFormatException e) {
                System.out.println("Invalid parameters\n");
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid parameters\n");
            }
        }
    }
}
