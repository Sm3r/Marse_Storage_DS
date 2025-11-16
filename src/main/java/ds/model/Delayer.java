package ds.model;

import ds.config.Settings
;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Delayer {
    private final ActorSystem system;
    private final Random rnd = new Random();

    public Delayer(ActorSystem system) {
        this.system = system;
    }

    private int gaussianDelay() {
        double val = Settings.meanMs + rnd.nextGaussian() * Settings.stddevMs;
        return Math.max((int) val, 0);
    }

    /** Normal send, no delay */
    public void msg(ActorRef target, Object message, ActorRef sender) {
        target.tell(message, sender);
    }

    /** Send with Gaussian random delay */
    public void delayedMsg(ActorRef target, Object message, ActorRef sender) {
        int delay = gaussianDelay();
        system.scheduler().scheduleOnce(
                Duration.create(delay, TimeUnit.MILLISECONDS),
                target, message,
                system.dispatcher(),
                sender
        );
    }
}
