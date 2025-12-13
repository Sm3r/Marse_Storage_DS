package ds.config;

import java.util.Scanner;

/*
Constraints that must be satisfied:
    1. N > 0
    2. R > 0 and R ≤ N
    3. W > 0 and W ≤ N
    4. R + W > N
    5. T > 0
 */
public class Settings {

    public static int N = 3;
    public static int R = 2;
    public static int W = 2;
    public static int T = 1000;

    // Simulated delay parameters
    public static final int meanMs = 40;
    public static final int stddevMs = 10;
    
    public static boolean validateConstraints() {
        StringBuilder errors = new StringBuilder();
        if (N <= 0) {
            errors.append("- N must be > 0 (current value: ").append(N).append(")\n");
        }
        if (R <= 0) {
            errors.append("- R must be > 0 (current value: ").append(R).append(")\n");
        }
        if (R > N) {
            errors.append("- R must be ≤ N (current: R=").append(R).append(", N=").append(N).append(")\n");
        }
        if (W <= 0) {
            errors.append("- W must be > 0 (current value: ").append(W).append(")\n");
        }
        if (W > N) {
            errors.append("- W must be ≤ N (current: W=").append(W).append(", N=").append(N).append(")\n");
        }
        if (R + W <= N) {
            errors.append("- R + W must be > N for consistency (current: R+W=").append(R+W)
                  .append(", N=").append(N).append(")\n");
        }
        if (T <= 0) {
            errors.append("- T must be > 0 (current value: ").append(T).append(")\n");
        }
        if (errors.length() > 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }
        
        return true;
    }
    
    public static void printConfiguration() {
        System.out.println("╔═════════════════════════════════╗");
        System.out.println("║  System Configuration Validated ║");
        System.out.println("╚═════════════════════════════════╝");
        System.out.println("  Replication Factor (N): " + N);
        System.out.println("  Read Quorum (R):        " + R);
        System.out.println("  Write Quorum (W):       " + W);
        System.out.println("  Timeout (T):            " + T + "ms");
        System.out.println("  Network Delay:          " + meanMs + "ms ± " + stddevMs + "ms");
        System.out.println();
    }
    
    public static void configure(Scanner scanner) {
        System.out.println("\n" + "=".repeat(15));
        System.out.println("Configuration");
        System.out.println("=".repeat(15));
        System.out.println();
        
        // Default values
        final int DEFAULT_N = 3;
        final int DEFAULT_R = 2;
        final int DEFAULT_W = 2;
        final int DEFAULT_T = 1000;
        
        boolean validConfig = false;
        
        while (!validConfig) {
            // Show defaults
            System.out.println("Default configuration: N=" + DEFAULT_N + ", R=" + DEFAULT_R + ", W=" + DEFAULT_W + ", T=" + DEFAULT_T + "ms");
            System.out.print("Use default configuration? (Y/n): ");
            
            String choice = scanner.nextLine().trim().toLowerCase();
            
            // Use temporary variables to avoid polluting Settings with invalid values
            int tempN = DEFAULT_N;
            int tempR = DEFAULT_R;
            int tempW = DEFAULT_W;
            int tempT = DEFAULT_T;
            
            try {
                if (!choice.isEmpty() && !choice.equals("y") && !choice.equals("yes")) {
                    // Custom configuration
                    System.out.println("\nEnter custom values (press ENTER to keep default):\n");
                    
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
