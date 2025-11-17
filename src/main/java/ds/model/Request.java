package ds.model;

import akka.actor.ActorRef;
import ds.model.Types.Result;

public class Request {

    public enum RequestType {
        GET,
        UPDATE,
        GET_JOIN,
    }

    private final ActorRef requester;
    private final RequestType type;
    private final int dataKey;
    private Result result;

    public Request(ActorRef requester, RequestType type, int dataKey) {
        this.requester = requester;
        this.type = type;
        this.dataKey = dataKey;
        this.result = null;
    }

    public ActorRef getRequester() {
        return requester;
    }

    public RequestType getType() {
        return type;
    }
    
    public int getDataKey() {
        return dataKey;
    }
    
    public Result getResult() {
        return result;
    }
    
    public void setResult(Result result) {
        this.result = result;
    }
    
    public boolean isCompleted() {
        return result != null;
    }

}


