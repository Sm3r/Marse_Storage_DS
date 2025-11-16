package ds.config;

public class Settings {
    // Replication factor
    public static final int N = 3;
    // Read quorum
    public static final int R = 2;
    // Write quorum
    public static final int W = 2;
    // Timeout in milliseconds
    public static final int T = 5000;

    // Simulation delay parameters
    public static final int meanMs = 100;
    public static final int stddevMs = 20;
}
