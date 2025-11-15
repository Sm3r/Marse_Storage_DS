package ds.actors;
import ds.data_structures.DataItem;
import ds.data_structures.Settings;
import ds.data_structures.Messages.OperationResult;
import ds.data_structures.ResponseResult;
import ds.data_structures.Messages.GetRequest;
import ds.data_structures.Messages.Response;
import ds.data_structures.Messages.UpdateRequest;


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

    // Node fields
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int op_id;
    private final ActorRef coordinator;
    private final ArrayList<ActorRef> nodes;
    private final ArrayList<DataItem> quorum;
    private final int data_key;
    private final String newValue;
    
    // Timeout message
    private static class OperationTimeout {}

    // Constructor
    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = null;
        scheduleTimeout();
        sendGetRequests(key);
    }

    public Handler(int op_id, ActorRef coordinator, ArrayList<ActorRef> nodes, ArrayList<DataItem> quorum, int key, String value) {
        this.op_id = op_id;
        this.coordinator = coordinator;
        this.nodes = nodes;
        this.quorum = quorum;
        this.data_key = key;
        this.newValue = value;
        scheduleTimeout();
        sendGetRequests(key);
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
    
    private void sendGetRequests(int id) {
        for (ActorRef node : nodes) {
            node.tell(new GetRequest(id), getSelf());
        }
    }

    private String getLatestValue() {
        String latestValue = quorum.stream()
            .max(Comparator.comparingLong(DataItem::getVersion))
            .map(DataItem::getValue)
            .orElse(null);
        return latestValue;
    }

    private long getLatestVersion(){
        long latestVersion = quorum.stream()
            .max(Comparator.comparingLong(DataItem::getVersion))
            .map(DataItem::getVersion)
            .orElse(0L);
        return latestVersion;
    }

    private void responseHandler(Response msg) {
        quorum.add(msg.value());
        if (quorum.size() >= (newValue == null ? Settings.R : Settings.W)) {
            if (newValue == null) {
                String latestValue = getLatestValue();
                log.info("Handler[{}]: Quorum achieved for GET request. Latest value: {}", op_id, latestValue);
                coordinator.tell(new OperationResult(op_id, new ResponseResult(true, latestValue)), getSelf());
            } else {
                log.info("Handler[{}]: Quorum achieved for UPDATE request.", op_id);
                long latestVersion = getLatestVersion();
                for (ActorRef node : nodes) {
                    node.tell(new UpdateRequest(data_key, new DataItem(newValue, latestVersion + 1)), getSelf());
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
                .match(Response.class, this::responseHandler)
                .match(OperationTimeout.class, this::handleTimeout)
                .build();
    }

}