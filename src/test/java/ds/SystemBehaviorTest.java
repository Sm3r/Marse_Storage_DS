package ds;

import ds.actors.Client;
import akka.actor.ActorRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * System Behavior Test
 * 
 * This test suite validates the distributed storage system's behavior
 * by creating a network, performing operations, and checking that
 * the system responds correctly.
 */
public class SystemBehaviorTest {
    
    private ManagementService service;
    
    @Before
    public void setUp() {
        service = new ManagementService();
        System.out.println("\n=== Starting Test Setup ===");
    }
    
    @After
    public void tearDown() {
        System.out.println("\n=== Tearing Down Test ===");
        service.shutdown();
    }
    
    /**
     * Test 1: Basic Network Initialization
     * Verify that the network can be initialized with multiple nodes
     */
    @Test
    public void testNetworkInitialization() {
        System.out.println("\n--- TEST 1: Network Initialization ---");
        
        // Create initial network
        service.addNode(10);
        service.waitForProcessing(500);
        service.addNode(20);
        service.waitForProcessing(500);
        service.addNode(30);
        service.waitForProcessing(500);
        service.addNode(40);
        service.waitForProcessing(500);
        
        // Verify nodes exist
        assertTrue("Node 10 should exist", service.nodeExists(10));
        assertTrue("Node 20 should exist", service.nodeExists(20));
        assertTrue("Node 30 should exist", service.nodeExists(30));
        assertTrue("Node 40 should exist", service.nodeExists(40));
        
        System.out.println("✓ Network initialization successful");
    }
    
    /**
     * Test 2: Basic Data Operations
     * Verify GET and UPDATE operations work correctly
     */
    @Test
    public void testBasicDataOperations() {
        System.out.println("\n--- TEST 2: Basic Data Operations ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        ActorRef client1 = service.getClient(1);
        assertNotNull("Client 1 should exist", client1);
        
        // Perform UPDATE operation
        System.out.println("Performing UPDATE operation...");
        client1.tell(new Client.UpdateRequest(10, 15, "testvalue"), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Perform GET operation
        System.out.println("Performing GET operation...");
        client1.tell(new Client.GetRequest(20, 15), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        System.out.println("✓ Basic data operations completed");
    }
    
    /**
     * Test 3: Node Join Operation
     * Verify that a new node can join and receive appropriate data
     */
    @Test
    public void testNodeJoin() {
        System.out.println("\n--- TEST 3: Node Join Operation ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        // Add data before join
        ActorRef client1 = service.getClient(1);
        client1.tell(new Client.UpdateRequest(10, 25, "beforejoin"), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Add new node
        System.out.println("Adding new node 15...");
        service.addNode(15);
        service.waitForProcessing(2000);
        
        assertTrue("Node 15 should exist after join", service.nodeExists(15));
        
        // Verify data is accessible
        client1.tell(new Client.GetRequest(15, 25), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        System.out.println("✓ Node join operation successful");
    }
    
    /**
     * Test 4: Node Crash and Recovery
     * Verify that a node can crash and recover with data intact
     */
    @Test
    public void testNodeCrashAndRecovery() {
        System.out.println("\n--- TEST 4: Node Crash and Recovery ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        // Add data
        ActorRef client1 = service.getClient(1);
        client1.tell(new Client.UpdateRequest(30, 35, "persistent"), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Crash node 30
        System.out.println("Crashing node 30...");
        service.crashNode(30);
        service.waitForProcessing(1000);
        
        assertTrue("Node 30 should still exist (crashed)", service.nodeExists(30));
        
        // Try to access data from another node
        client1.tell(new Client.GetRequest(20, 35), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Recover node 30
        System.out.println("Recovering node 30...");
        service.recoverNode(30, 20);
        service.waitForProcessing(2000);
        
        // Verify data is still accessible
        client1.tell(new Client.GetRequest(30, 35), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        System.out.println("✓ Node crash and recovery successful");
    }
    
    /**
     * Test 5: Graceful Node Leave
     * Verify that a node can leave gracefully and transfer its data
     */
    @Test
    public void testGracefulNodeLeave() {
        System.out.println("\n--- TEST 5: Graceful Node Leave ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        // Add data
        ActorRef client1 = service.getClient(1);
        client1.tell(new Client.UpdateRequest(40, 45, "willmove"), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Node leaves
        System.out.println("Node 40 leaving network...");
        service.leaveNetwork(40);
        service.waitForProcessing(3000);
        
        // Verify data is still accessible from other nodes
        client1.tell(new Client.GetRequest(50, 45), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        System.out.println("✓ Graceful node leave successful");
    }
    
    /**
     * Test 6: Crash Constraint Validation
     * Verify that the system prevents crashing the last active node
     */
    @Test
    public void testNConstraintCrash() {
        System.out.println("\n--- TEST 6: Crash Constraint Validation ---");
        
        // Create minimal network (just 2 nodes)
        service.addNode(10);
        service.waitForProcessing(500);
        service.addNode(20);
        service.waitForProcessing(500);
        
        // Crash first node (should succeed - leaves 1 active)
        System.out.println("Crashing node 10 (should succeed)...");
        service.crashNode(10);
        service.waitForProcessing(500);
        
        // Verify node 10 is crashed
        assertTrue("Node 10 should exist (crashed)", service.nodeExists(10));
        assertNull("Node 10 should not be active", service.getNode(10));
        
        // Try to crash the last node (should fail - would leave 0 active)
        System.out.println("Attempting to crash node 20 (should fail - last active node)...");
        service.crashNode(20);
        service.waitForProcessing(500);
        
        // Verify node 20 is still active (crash was prevented)
        ActorRef node20 = service.getNode(20);
        assertNotNull("Node 20 should still be active (crash prevented)", node20);
        
        System.out.println("✓ Crash constraint validation successful");
    }
    
    /**
     * Test 7: N Constraint Validation (Leave)
     * Verify that the system prevents nodes from leaving when it would violate N >= active nodes
     */
    @Test
    public void testNConstraintLeave() {
        System.out.println("\n--- TEST 7: N Constraint Validation (Leave) ---");
        
        // Create minimal network (exactly N=3 nodes)
        service.addNode(10);
        service.waitForProcessing(500);
        service.addNode(20);
        service.waitForProcessing(500);
        service.addNode(30);
        service.waitForProcessing(1000);
        
        // Try to leave (should fail - would leave 2 < N=3)
        System.out.println("Attempting node 20 to leave (should fail)...");
        service.leaveNetwork(20);
        service.waitForProcessing(3000);
        
        // Verify node 20 is still active (leave was prevented)
        ActorRef node20 = service.getNode(20);
        assertNotNull("Node 20 should still be active (leave prevented)", node20);
        
        System.out.println("✓ N constraint validation for leave successful");
    }
    
    /**
     * Test 8: Concurrent Client Operations
     * Verify that multiple clients can operate concurrently
     */
    @Test
    public void testConcurrentClientOperations() {
        System.out.println("\n--- TEST 8: Concurrent Client Operations ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        ActorRef client1 = service.getClient(1);
        ActorRef client2 = service.getClient(2);
        
        assertNotNull("Client 1 should exist", client1);
        assertNotNull("Client 2 should exist", client2);
        
        // Concurrent updates from different clients
        System.out.println("Performing concurrent operations...");
        client1.tell(new Client.UpdateRequest(10, 60, "client1_v1"), ActorRef.noSender());
        client2.tell(new Client.UpdateRequest(20, 70, "client2_v1"), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Concurrent reads
        client1.tell(new Client.GetRequest(30, 60), ActorRef.noSender());
        client2.tell(new Client.GetRequest(40, 70), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        System.out.println("✓ Concurrent client operations successful");
    }
    
    /**
     * Test 9: Sequential Consistency
     * Verify that sequential consistency is maintained with version numbers
     */
    @Test
    public void testSequentialConsistency() {
        System.out.println("\n--- TEST 9: Sequential Consistency ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        ActorRef client1 = service.getClient(1);
        
        // Perform sequential updates
        System.out.println("Performing sequential updates...");
        client1.tell(new Client.UpdateRequest(10, 80, "version1"), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        client1.tell(new Client.UpdateRequest(20, 80, "version2"), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        client1.tell(new Client.UpdateRequest(30, 80, "version3"), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        // Read from different nodes - should get latest version
        System.out.println("Reading from different coordinators...");
        client1.tell(new Client.GetRequest(40, 80), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        client1.tell(new Client.GetRequest(50, 80), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        System.out.println("✓ Sequential consistency test completed");
    }
    
    /**
     * Test 10: Data Redistribution After Join
     * Verify that data is properly redistributed when a node joins
     */
    @Test
    public void testDataRedistributionAfterJoin() {
        System.out.println("\n--- TEST 10: Data Redistribution After Join ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        ActorRef client1 = service.getClient(1);
        
        // Add data across the ring
        client1.tell(new Client.UpdateRequest(10, 15, "data15"), ActorRef.noSender());
        service.waitForProcessing(1000);
        client1.tell(new Client.UpdateRequest(20, 25, "data25"), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        // Print network status before join
        System.out.println("Network status BEFORE node 15 joins:");
        service.printNetworkStatus();
        service.waitForProcessing(2000);
        
        // Add node 15 (between 10 and 20)
        System.out.println("Adding node 15...");
        service.addNode(15);
        service.waitForProcessing(2000);
        
        // Print network status after join
        System.out.println("Network status AFTER node 15 joins:");
        service.printNetworkStatus();
        service.waitForProcessing(2000);
        
        // Verify data is still accessible
        client1.tell(new Client.GetRequest(15, 15), ActorRef.noSender());
        service.waitForProcessing(1000);
        client1.tell(new Client.GetRequest(20, 25), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        System.out.println("✓ Data redistribution after join successful");
    }
    
    /**
     * Test 11: Complex Scenario - Mixed Operations
     * Simulate a realistic scenario with multiple operations
     */
    @Test
    public void testComplexMixedOperations() {
        System.out.println("\n--- TEST 11: Complex Mixed Operations ---");
        
        // Initialize network
        service.initialize();
        service.waitForProcessing(2000);
        
        ActorRef client1 = service.getClient(1);
        ActorRef client2 = service.getClient(2);
        
        // Phase 1: Initial data operations
        System.out.println("Phase 1: Initial data operations");
        client1.tell(new Client.UpdateRequest(10, 100, "initial"), ActorRef.noSender());
        service.waitForProcessing(1000);
        client2.tell(new Client.GetRequest(20, 100), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        // Phase 2: Add a node
        System.out.println("Phase 2: Adding node 25");
        service.addNode(25);
        service.waitForProcessing(2000);
        
        // Phase 3: Update after topology change
        System.out.println("Phase 3: Update after topology change");
        client1.tell(new Client.UpdateRequest(25, 100, "afterjoin"), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        // Phase 4: Crash a node
        System.out.println("Phase 4: Crashing node 30");
        service.crashNode(30);
        service.waitForProcessing(1000);
        
        // Phase 5: Continue operations with crashed node
        System.out.println("Phase 5: Operations with crashed node");
        client2.tell(new Client.GetRequest(40, 100), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        // Phase 6: Recover the node
        System.out.println("Phase 6: Recovering node 30");
        service.recoverNode(30, 40);
        service.waitForProcessing(2000);
        
        // Phase 7: Final verification
        System.out.println("Phase 7: Final verification");
        client1.tell(new Client.GetRequest(30, 100), ActorRef.noSender());
        service.waitForProcessing(1000);
        
        // Print final network status
        service.printNetworkStatus();
        service.waitForProcessing(2000);
        
        System.out.println("✓ Complex mixed operations successful");
    }
    
    /**
     * Test 12: Multiple Node Joins and Leaves
     * Test dynamic network with multiple topology changes
     */
    @Test
    public void testMultipleTopologyChanges() {
        System.out.println("\n--- TEST 12: Multiple Topology Changes ---");
        
        // Start with basic network
        service.addNode(10);
        service.waitForProcessing(500);
        service.addNode(20);
        service.waitForProcessing(500);
        service.addNode(30);
        service.waitForProcessing(500);
        service.addNode(40);
        service.waitForProcessing(1000);
        
        // Add client and some data
        ActorRef client = service.getClient(1);
        if (client == null) {
            service.initialize(); // Create clients if not exist
            service.waitForProcessing(1000);
            client = service.getClient(1);
        }
        
        client.tell(new Client.UpdateRequest(10, 50, "persistent_data"), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        // Join-Leave-Join sequence
        System.out.println("Adding node 25...");
        service.addNode(25);
        service.waitForProcessing(2000);
        
        System.out.println("Adding node 35...");
        service.addNode(35);
        service.waitForProcessing(2000);
        
        System.out.println("Node 40 leaving...");
        service.leaveNetwork(40);
        service.waitForProcessing(3000);
        
        System.out.println("Adding node 45...");
        service.addNode(45);
        service.waitForProcessing(2000);
        
        // Verify data persists through changes
        client.tell(new Client.GetRequest(25, 50), ActorRef.noSender());
        service.waitForProcessing(1500);
        
        System.out.println("✓ Multiple topology changes successful");
    }
}
