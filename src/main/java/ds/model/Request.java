package ds.model;

import akka.actor.ActorRef;
import ds.model.Types.ResponseResult;

public class Request {
    private final ActorRef requester;
    private final RequestType type;
    private ResponseResult result;

    public Request(ActorRef requester, RequestType type) {
        this.requester = requester;
        this.type = type;
        this.result = null;
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
