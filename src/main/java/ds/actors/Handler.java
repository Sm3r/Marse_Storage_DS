package ds.actors;
import ds.config.Settings;
import ds.model.Delayer;
import ds.model.Types.*;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

// Handler actor
public class Handler extends AbstractActor {

    // Handler fields
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int op_id;
    private final ActorRef coordinator;
    private final ArrayList<ActorRef> nodes;
    private final ArrayList<DataItem> quorum;
    private final int data_key;
    private final String newValue;
    private final Delayer delayer;
    private final boolean coordinatorIsReplica;
    private final long coordinatorLamportClock;  // Lamport clock from coordinator
    private final int coordinatorNodeId;  // Node ID for tie-breaking
    private int responsesReceived = 0;
    private long maxLamportClock = 0;  // Track max Lamport clock from responses

    // Constructor
    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, boolean coordinatorIsReplica, Delayer delayer, long lamportClock, int nodeId) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = null;
        this.delayer = delayer;
        this.coordinatorIsReplica = coordinatorIsReplica;
        this.coordinatorLamportClock = lamportClock;
        this.coordinatorNodeId = nodeId;
        this.maxLamportClock = lamportClock;
        scheduleTimeout();
        sendReadDataRequests(key);
    }

    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, String value, boolean coordinatorIsReplica, Delayer delayer, long lamportClock, int nodeId) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = value;
        this.delayer = delayer;
        this.coordinatorIsReplica = coordinatorIsReplica;
        this.coordinatorLamportClock = lamportClock;
        this.coordinatorNodeId = nodeId;
        this.maxLamportClock = lamportClock;
        log.info("Handler[{}]: Created for UPDATE on key {} (replicas={}, lamport={})", op_id, key, nodes.size(), lamportClock);
        scheduleTimeout();
        sendReadDataRequests(key);
    }

    // Functions
    private void scheduleTimeout() {
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(Settings.T, TimeUnit.MILLISECONDS),
            getSelf(),
            new OperationTimeout(),
            getContext().getSystem().dispatcher(),
            getSelf()
        );
    }
    
    private void sendReadDataRequests(int id) {
        for (ActorRef node : nodes) {
            delayer.delayedMsg(getSelf(), new ReadDataRequest(id, coordinatorLamportClock), node);
        }
    }

    private DataItem getLatestDataItem(){
        // Use total ordering: compare version first, then nodeId as tie-breaker
        return quorum.stream()
            .max(Comparator.comparingLong(DataItem::version)
                          .thenComparingInt(DataItem::nodeId))
            .orElse(new DataItem(null, 0L, 0));
    }

    private void handleReadDataResponse(ReadDataResponse msg) {
        responsesReceived++;
        // Track maximum Lamport clock from responses for sequential consistency
        maxLamportClock = Math.max(maxLamportClock, msg.lamportClock());
        log.info("Handler[{}]: Received ReadDataResponse (lamport={}, responses={})", op_id, msg.lamportClock(), responsesReceived + "/" + nodes.size());
        if (msg.value() != null) {
            quorum.add(msg.value());
        }
        
        int requiredQuorum = (newValue == null ? Settings.R : Settings.W);
        if (responsesReceived >= requiredQuorum) {
            if (newValue == null) {
                // GET operation - return value with highest (version, nodeId)
                DataItem latestItem = getLatestDataItem();
                String latestValue = latestItem.value();
                log.info("Handler[{}]: Read quorum achieved. Latest: {} (v={}, n={})", op_id, latestValue, latestItem.version(), latestItem.nodeId());
                coordinator.tell(new Result(op_id, latestItem), getSelf());
            } else {
                // UPDATE operation - use max Lamport clock + 1 for new version
                log.info("Handler[{}]: Write quorum of responses achieved for update operation.", op_id);
                DataItem latestItem = getLatestDataItem();
                // New version is max of (latest version, max Lamport clock from responses) + 1
                long newVersion = Math.max(latestItem.version(), maxLamportClock) + 1;
                DataItem updatedItem = new DataItem(newValue, newVersion, coordinatorNodeId);
                
                log.info("Handler[{}]: Writing (v={}, n={}) - total order", op_id, newVersion, coordinatorNodeId);
                
                // Send write requests to all replica nodes
                for (ActorRef node : nodes) {
                    delayer.delayedMsg(getSelf(), new WriteDataRequest(data_key, updatedItem), node);
                }
                
                // If coordinator is also a replica, update its data too
                if (coordinatorIsReplica) {
                    delayer.delayedMsg(getSelf(), new WriteDataRequest(data_key, updatedItem), coordinator);
                }
                
                coordinator.tell(new Result(op_id, new DataItem("UPDATE_SUCCESS", newVersion, coordinatorNodeId)), getSelf());
            }
            getContext().stop(getSelf());
        }
    }
    
    private void handleTimeout(OperationTimeout msg) {
        log.warning("Handler[{}]: Operation timeout occurred", op_id);
        coordinator.tell(new Result(op_id, null), getSelf());
        getContext().stop(getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadDataResponse.class, this::handleReadDataResponse)
                .match(OperationTimeout.class, this::handleTimeout)
                .build();
    }

}