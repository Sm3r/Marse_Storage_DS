package ds.data_structures;

import java.util.Map;

import akka.actor.ActorRef;

// Message types as records
public class Messages {

    // Client to Node messages
    public record Get(int key) {}
    public record Update(int key, String value) {}
    public record SetPeers(Map<Integer, ActorRef> peers) {}
    public record UpdatePeer(int id, ActorRef peer) {}
    public record Print() {}
    public record PrintPeers() {}

    // Node to Client messages
    public record GetResponse(int key, DataItem value) {}
    public record UpdateResponse(int key, boolean success) {}

    // Node to Handler messages
    public record Response(DataItem value) {}

    // Handler to Node messages
    public record GetRequest(int key) {}
    public record UpdateRequest(int key, DataItem dataItem) {}
    public record OperationResult(int op_id, ResponseResult result) {}
}
