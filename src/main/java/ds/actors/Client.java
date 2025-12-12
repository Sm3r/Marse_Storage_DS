package ds.actors;

import ds.model.Delayer;
import ds.model.Types.ClientGetRequest;
import ds.model.Types.ClientUpdateRequest;
import ds.model.Types.Result;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.Map;

// Client actor
public class Client extends AbstractActor {

    // Message types as records
    public record GetRequest(int nodeId, int key) {}
    public record UpdateRequest(int nodeId, int key, String value) {}

    // Client fields
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private final Delayer delayer;
    private final Map<Integer, ActorRef> nodes;
    

    // Constructor
    public Client(int id, Map<Integer, ActorRef> nodeMap, Delayer delayer) {
        this.id = id;
        this.delayer = delayer;
        this.nodes = nodeMap;
    }

    // Handle GET/UPDATE request from main
    private void handleGetRequest(GetRequest msg) {
        ActorRef node = nodes.get(msg.nodeId());
        if (node != null) {
            log.info("Client[{}]: Sending GET request for key {} to node {}", id, msg.key(), msg.nodeId());
            delayer.delayedMsg(getSelf(), new ClientGetRequest(msg.key()), node);
        } else {
            log.warning("Client[{}]: Node {} not found for GET request", id, msg.nodeId());
        }
    }

    private void handleUpdateRequest(UpdateRequest msg) {
        ActorRef node = nodes.get(msg.nodeId());
        if (node != null) {
            log.info("Client[{}]: Sending UPDATE request for key {} with value '{}' to node {}", id, msg.key(), msg.value(), msg.nodeId());
            delayer.delayedMsg(getSelf(), new ClientUpdateRequest(msg.key(), msg.value()), node);
        } else {
            log.warning("Client[{}]: Node {} not found for UPDATE request", id, msg.nodeId());
        }
    }

    // Handle Result response from node
    private void handleResult(Result msg) {
        if (msg.value() != null) {
            String output = String.format("Client[%d]: Received result for operation %d - Value: '%s' (version: %d, nodeId: %d)", 
                id, msg.op_id(), msg.value().value(), msg.value().version(), msg.value().nodeId());
            log.info(output);
            System.out.println(output);
        } else {
            String output = String.format("Client[%d]: Received result for operation %d - Operation failed (timeout or error)", 
                id, msg.op_id());
            log.warning(output);
            System.out.println(output);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetRequest.class, this::handleGetRequest)
                .match(UpdateRequest.class, this::handleUpdateRequest)
                .match(Result.class, this::handleResult)
                .build();
    }
}