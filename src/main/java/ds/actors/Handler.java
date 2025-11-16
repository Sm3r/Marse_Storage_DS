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

    // Constructor
    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, Delayer delayer) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = null;
        this.delayer = delayer;
        scheduleTimeout();
        sendReadDataRequests(key);
    }

    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, String value, Delayer delayer) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = value;
        this.delayer = delayer;
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
        if (msg.value() != null) {
            quorum.add(msg.value());
        }
        if (quorum.size() >= (newValue == null ? Settings.R : Settings.W)) {
            if (newValue == null) {
                String latestValue = getLatestValue();
                log.info("Handler[{}]: Read quorum achieved. Latest value: {}", op_id, latestValue);
                coordinator.tell(new OperationResult(op_id, new ResponseResult(true, latestValue)), getSelf());
            } else {
                log.info("Handler[{}]: Read quorum achieved for update operation.", op_id);
                long latestVersion = getLatestVersion();
                for (ActorRef node : nodes) {
                    node.tell(new WriteDataRequest(data_key, new DataItem(newValue, latestVersion + 1)), getSelf());
                }
                coordinator.tell(new OperationResult(op_id, new ResponseResult(true, "UPDATE_SUCCESS")), getSelf());
            }
            getContext().stop(getSelf());
        }
    }
    
    private void handleTimeout(OperationTimeout msg) {
        log.warning("Handler[{}]: Operation timeout occurred", op_id);
        coordinator.tell(new OperationResult(op_id, new ResponseResult(false, "TIMEOUT")), getSelf());
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