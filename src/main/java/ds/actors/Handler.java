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
    private int responsesReceived = 0;

    // Constructor
    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, boolean coordinatorIsReplica, Delayer delayer) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = null;
        this.delayer = delayer;
        this.coordinatorIsReplica = coordinatorIsReplica;
        scheduleTimeout();
        sendReadDataRequests(key);
    }

    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, String value, boolean coordinatorIsReplica, Delayer delayer) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = value;
        this.delayer = delayer;
        this.coordinatorIsReplica = coordinatorIsReplica;
        log.info("Handler[{}]: Created for UPDATE operation on key {} with {} replica nodes (coordinator is replica: {})", op_id, key, nodes.size(), coordinatorIsReplica);
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
            delayer.delayedMsg(node, new ReadDataRequest(id), getSelf());
        }
    }

    private String getLatestValue() {
        String latestValue = quorum.stream()
            .max(Comparator.comparingLong(DataItem::version))
            .map(DataItem::value)
            .orElse(null);
        return latestValue;
    }

    private long getLatestVersion(){
        long latestVersion = quorum.stream()
            .max(Comparator.comparingLong(DataItem::version))
            .map(DataItem::version)
            .orElse(0L);
        return latestVersion;
    }

    private void handleReadDataResponse(ReadDataResponse msg) {
        responsesReceived++;
        log.info("Handler[{}]: Received ReadDataResponse (value={}, responses={}/{})", op_id, msg.value(), responsesReceived, nodes.size());
        if (msg.value() != null) {
            quorum.add(msg.value());
        }
        
        int requiredQuorum = (newValue == null ? Settings.R : Settings.W);
        if (responsesReceived >= requiredQuorum) {
            if (newValue == null) {
                // GET operation
                String latestValue = getLatestValue();
                log.info("Handler[{}]: Read quorum achieved. Latest value: {}", op_id, latestValue);
                coordinator.tell(new Result(op_id, new DataItem(latestValue, getLatestVersion())), getSelf());
            } else {
                // UPDATE operation
                log.info("Handler[{}]: Write quorum of responses achieved for update operation.", op_id);
                long latestVersion = getLatestVersion();
                DataItem updatedItem = new DataItem(newValue, latestVersion + 1);
                
                // Send write requests to all replica nodes
                for (ActorRef node : nodes) {
                    node.tell(new WriteDataRequest(data_key, updatedItem), getSelf());
                }
                
                // If coordinator is also a replica, update its data too
                if (coordinatorIsReplica) {
                    coordinator.tell(new WriteDataRequest(data_key, updatedItem), getSelf());
                }
                
                coordinator.tell(new Result(op_id, new DataItem("UPDATE_SUCCESS", latestVersion + 1)), getSelf());
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