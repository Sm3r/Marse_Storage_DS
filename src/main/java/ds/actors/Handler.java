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
    private final long coordinatorClock;  // Clock from coordinator
    private final int coordinatorNodeId;  // Node ID for tie-breaking
    private int responsesReceived = 0;
    private long maxClock = 0;  // Track max clock from responses

    // Constructor
    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, boolean coordinatorIsReplica, Delayer delayer, long clock, int nodeId) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = null;
        this.delayer = delayer;
        this.coordinatorIsReplica = coordinatorIsReplica;
        this.coordinatorClock = clock;
        this.coordinatorNodeId = nodeId;
        this.maxClock = clock;
        scheduleTimeout();
        sendReadDataRequests(key);
    }

    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, String value, boolean coordinatorIsReplica, Delayer delayer, long clock, int nodeId) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = value;
        this.delayer = delayer;
        this.coordinatorIsReplica = coordinatorIsReplica;
        this.coordinatorClock = clock;
        this.coordinatorNodeId = nodeId;
        this.maxClock = clock;
        log.info("Handler[{}]: Created for UPDATE on key {} (replicas={}, clock={})", op_id, key, nodes.size(), clock);
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
            delayer.delayedMsg(getSelf(), new ReadDataRequest(id, coordinatorClock), node);
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
        // Track maximum clock from responses for sequential consistency
        maxClock = Math.max(maxClock, msg.clock());
        log.info("Handler[{}]: Received ReadDataResponse (clock={}, responses={})", op_id, msg.clock(), responsesReceived + "/" + nodes.size());
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
                // UPDATE operation - use max clock + 1 for new version
                log.info("Handler[{}]: Write quorum of responses achieved for update operation.", op_id);
                DataItem latestItem = getLatestDataItem();
                // New version is max of (latest version, max clock from responses) + 1
                long newVersion = Math.max(latestItem.version(), maxClock) + 1;
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