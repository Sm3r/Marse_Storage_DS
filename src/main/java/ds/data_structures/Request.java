package ds.data_structures;

import akka.actor.ActorRef;

public class Request {
    private int id;
    private ActorRef requester;
    private RequestType type;
    private ResponseResult result;

    public Request(int id, ActorRef requester, RequestType type) {
        this.id = id;
        this.requester = requester;
        this.type = type;
        this.result = null;
    }

    public void addResponseResult(ResponseResult result) {
        this.result = result;
    }
}
