package ds.model;

import java.util.Map;
import akka.actor.ActorRef;

public class Types {
    
    // ==================== Data Models ====================
    
    public record DataItem(String value, long version) {
        public DataItem(String value) {
            this(value, 0L);
        }
    }
    
    public record ResponseResult(boolean success, String value) {}
    
    // ==================== Client to Node Messages ====================
    
    public record Get(int key) {}
    public record Update(int key, String value) {}
    public record SetPeers(Map<Integer, ActorRef> peers) {}
    public record UpdatePeer(int id, ActorRef peer) {}
    public record Print() {}
    public record PrintPeers() {}
    
    // ==================== Node to Client Messages ====================
    
    public record GetResponse(int key, DataItem value) {}
    public record UpdateResponse(int key, boolean success) {}
    
    // ==================== Node to Handler Messages ====================
    
    public record Response(DataItem value) {}
    
    // ==================== Handler to Node Messages ====================
    
    public record GetRequest(int key) {}
    public record UpdateRequest(int key, DataItem dataItem) {}
    public record OperationResult(int op_id, ResponseResult result) {}
    
    // ==================== Handler Internal Messages ====================
    
    public record OperationTimeout() {}
}
