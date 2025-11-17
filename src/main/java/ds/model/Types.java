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
    
    // ==================== Client -> Coordinator Node Messages ====================
    
    public record ClientGetRequest(int key) {}
    public record ClientUpdateRequest(int key, String value) {}
    public record SetPeers(Map<Integer, ActorRef> peers) {}
    public record AddPeer(int id, ActorRef peer) {}
    public record Print() {}
    public record PrintPeers() {}
    
    // ==================== Coordinator Node -> Client Messages ====================
    
    public record ClientGetResponse(int key, DataItem value) {}
    public record ClientUpdateResponse(int key, boolean success) {}
    
    // ==================== Handler -> Replica Node Messages ====================
    
    public record ReadDataRequest(int key) {}
    public record WriteDataRequest(int key, DataItem dataItem) {}
    
    // ==================== Replica Node -> Handler Messages ====================
    
    public record ReadDataResponse(DataItem value) {}
    
    // ==================== Handler -> Coordinator Node Messages ====================
    
    public record Result(int op_id, DataItem value) {}
    
    // ==================== Handler Internal Messages ====================
    
    public record OperationTimeout() {}

    // ==================== Joining operation ====================
    public record JoinRequest(int nodeId, ActorRef nodeRef) {}
    public record RegisterPeers(Map<Integer, ActorRef> peers) {}
    public record GetAllDataItems(int nodeId) {}
    public record SendAllDataItems(Map<Integer, DataItem> dataItems) {}
}
