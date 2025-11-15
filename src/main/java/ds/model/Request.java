package ds.model;

import akka.actor.ActorRef;
import ds.model.Types.ResponseResult;

public class Request {
    private final int id;
    private final ActorRef requester;
    private final RequestType type;
    private ResponseResult result;

    public Request(int id, ActorRef requester, RequestType type) {
        this.id = id;
        this.requester = requester;
        this.type = type;
        this.result = null;
    }

    public int getId() {
        return id;
    }

    public ActorRef getRequester() {
        return requester;
    }

    public RequestType getType() {
        return type;
    }

    public ResponseResult getResult() {
        return result;
    }

    public void addResponseResult(ResponseResult result) {
        this.result = result;
    }
}
