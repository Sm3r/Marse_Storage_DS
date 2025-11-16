package ds.actors;

import ds.model.Delayer;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.TreeMap;
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
        this.nodes = new TreeMap<>(nodeMap); 
    }

    // Handle GET request from client
    private void handleGetRequest(GetRequest msg) {
        ActorRef node = nodes.get(msg.nodeId());
        if (node != null) {
            log.info("Client[{}]: Sending GET request for key {} to node {}", id, msg.key(), msg.nodeId());
            delayer.delayedMsg(node, new ds.model.Types.ClientGetRequest(msg.key()), getSelf());
        } else {
            log.warning("Client[{}]: Node {} not found for GET request", id, msg.nodeId());
        }
    }

    // Handle UPDATE request from client
    private void handleUpdateRequest(UpdateRequest msg) {
        ActorRef node = nodes.get(msg.nodeId());
        if (node != null) {
            log.info("Client[{}]: Sending UPDATE request for key {} with value '{}' to node {}", id, msg.key(), msg.value(), msg.nodeId());
            delayer.delayedMsg(node, new ds.model.Types.ClientUpdateRequest(msg.key(), msg.value()), getSelf());
        } else {
            log.warning("Client[{}]: Node {} not found for UPDATE request", id, msg.nodeId());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetRequest.class, this::handleGetRequest)
                .match(UpdateRequest.class, this::handleUpdateRequest)
                .build();
    }
}