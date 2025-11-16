package ds.actors;

import ds.model.Delayer;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.TreeMap;
import java.util.Map;

// Client actor
public class Client extends AbstractActor {

    // Message types as records
    public record AddNode(int nodeId) {}
    public record RemoveNode(int nodeId) {}
    public record NodeMessage(int nodeId, Object message) {}
    public record PrintNodes() {}

    // Client fields
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private final Map<Integer, ActorRef> nodes;
    private final Delayer delayer;

    // Constructor
    public Client(int id, Map<Integer, ActorRef> nodeMap, Delayer delayer) {
        this.id = id;
        this.nodes = new TreeMap<>(nodeMap);
        this.delayer = delayer;
    }

    // Add a new node as a child
    private void handleAddNode(AddNode msg) {
        if (!nodes.containsKey(msg.nodeId())) {
            ActorRef node = getContext().actorOf(
                Props.create(Node.class, () -> new Node(msg.nodeId(), delayer)),
                "node" + msg.nodeId()
            );
            nodes.put(msg.nodeId(), node);
            log.info("Client[{}]: Node added -> {}", id, msg.nodeId());
        } else {
            log.warning("Client[{}]: Node already exists -> {}", id, msg.nodeId());
        }
    }

    // Remove an existing child node
    private void handleRemoveNode(RemoveNode msg) {
        ActorRef node = nodes.remove(msg.nodeId());
        if (node != null) {
            getContext().stop(node);
            log.info("Client[{}]: Node removed -> {}", id, msg.nodeId());
        } else {
            log.warning("Client[{}]: Node not found -> {}", id, msg.nodeId());
        }
    }

    // Forward messages to a specific node
    private void handleNodeMessage(NodeMessage msg) {
        ActorRef node = nodes.get(msg.nodeId());
        if (node != null) {
            log.debug("Client[{}]: Forwarding message {} to node {}", id, msg.message().getClass().getSimpleName(), msg.nodeId());
            delayer.delayedMsg(node, msg.message(), getSelf());
        } else {
            log.warning("Client[{}]: Node not found -> {}", id, msg.nodeId());
        }
    }

    // Print all nodes
    private void handlePrintNodes(PrintNodes msg) {
        log.info("Client[{}]: Active nodes -> {}", id, nodes.keySet());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AddNode.class, this::handleAddNode)
                .match(RemoveNode.class, this::handleRemoveNode)
                .match(NodeMessage.class, this::handleNodeMessage)
                .match(PrintNodes.class, this::handlePrintNodes)
                .build();
    }
}