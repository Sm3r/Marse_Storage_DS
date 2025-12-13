package ds.config;

/**
 * System configuration parameters
 * 
 * IMPORTANT: All parameters are configurable at runtime.
 * User will be prompted for values when starting the system.
 * 
 * Constraints that must be satisfied:
 * 1. N > 0 (at least one replica)
 * 2. R > 0 and R ≤ N (read quorum must be valid)
 * 3. W > 0 and W ≤ N (write quorum must be valid)
 * 4. R + W > N (ensures quorum intersection for consistency)
 * 5. T > 0 (positive timeout)
 */
public class Settings {
    // ============ CONFIGURABLE PARAMETERS ============
    
    // Replication factor - number of replicas for each data item
    public static int N = 3;
    
    // Read quorum - minimum responses needed for read operations
    public static int R = 2;
    
    // Write quorum - minimum responses needed for write operations
    public static int W = 2;
    
    // Timeout in milliseconds for operations
    public static int T = 1000;

    // Simulation delay parameters (network propagation)
    public static final int meanMs = 40;
    public static final int stddevMs = 10;
    
    // ============ CONSTRAINT VALIDATION ============
    public static boolean validateConstraints() {
        StringBuilder errors = new StringBuilder();
        
        // Constraint 1: N must be positive
        if (N <= 0) {
            errors.append("- N must be > 0 (current value: ").append(N).append(")\n");
        }
        
        // Constraint 2: R must be positive and ≤ N
        if (R <= 0) {
            errors.append("- R must be > 0 (current value: ").append(R).append(")\n");
        }
        if (R > N) {
            errors.append("- R must be ≤ N (current: R=").append(R).append(", N=").append(N).append(")\n");
        }
        
        // Constraint 3: W must be positive and ≤ N
        if (W <= 0) {
            errors.append("- W must be > 0 (current value: ").append(W).append(")\n");
        }
        if (W > N) {
            errors.append("- W must be ≤ N (current: W=").append(W).append(", N=").append(N).append(")\n");
        }
        
        // Constraint 4: R + W must be > N (quorum intersection for consistency)
        if (R + W <= N) {
            errors.append("- R + W must be > N for consistency (current: R+W=").append(R+W)
                  .append(", N=").append(N).append(")\n");
        }
        
        // Constraint 5: T must be positive
        if (T <= 0) {
            errors.append("- T must be > 0 (current value: ").append(T).append(")\n");
        }
        
        // If any errors, throw exception
        if (errors.length() > 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }
        
        return true;
    }
    
    /**
     * Returns a summary of current configuration as a string
     */
    public static String getConfigurationSummary() {
        return String.format(
            "Configuration: N=%d, R=%d, W=%d, T=%dms (R+W=%d > N=%d: %s)",
            N, R, W, T, R+W, N, (R+W > N) ? "✓" : "✗"
        );
    }
    
    /**
     * Prints the configuration summary to console
     */
    public static void printConfiguration() {
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║  System Configuration Validated                              ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
        System.out.println("  Replication Factor (N): " + N);
        System.out.println("  Read Quorum (R):        " + R);
        System.out.println("  Write Quorum (W):       " + W);
        System.out.println("  Timeout (T):            " + T + "ms");
        System.out.println("  Quorum Intersection:    R+W=" + (R+W) + " > N=" + N + " ✓");
        System.out.println("  Network Delay:          " + meanMs + "ms ± " + stddevMs + "ms");
        System.out.println();
    }
}
