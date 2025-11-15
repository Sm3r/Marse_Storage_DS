package ds.actors;

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

    // Constructor
    public Client(int id, Map<Integer, ActorRef> nodeMap) {
        this.id = id;
        this.nodes = new TreeMap<>(nodeMap);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // Add a new node as a child
                .match(AddNode.class, msg -> {
                    if (!nodes.containsKey(msg.nodeId())) {
                        ActorRef node = getContext().actorOf(
                            Props.create(Node.class, () -> new Node(msg.nodeId())),
                            "node" + msg.nodeId()
                        );
                        nodes.put(msg.nodeId(), node);
                        log.info("Client[{}]: Node added -> {}", id, msg.nodeId());
                    } else {
                        log.warning("Client[{}]: Node already exists -> {}", id, msg.nodeId());
                    }
                })
                // Remove an existing child node
                .match(RemoveNode.class, msg -> {
                    ActorRef node = nodes.remove(msg.nodeId());
                    if (node != null) {
                        getContext().stop(node);
                        log.info("Client[{}]: Node removed -> {}", id, msg.nodeId());
                    } else {
                        log.warning("Client[{}]: Node not found -> {}", id, msg.nodeId());
                    }
                })
                // Forward messages to a specific node
                .match(NodeMessage.class, msg -> {
                    ActorRef node = nodes.get(msg.nodeId());
                    if (node != null) {
                        log.debug("Client[{}]: Forwarding message {} to node {}", id, msg.message().getClass().getSimpleName(), msg.nodeId());
                        node.tell(msg.message(), getSelf());
                    } else {
                        log.warning("Client[{}]: Node not found -> {}", id, msg.nodeId());
                    }
                })
                // Print all nodes
                .match(PrintNodes.class, msg -> log.info("Client[{}]: Active nodes -> {}", id, nodes.keySet()))
                .build();
    }
}