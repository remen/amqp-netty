package nettyamqp;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyTesting {

    public static class IncomingMessage {
        public final String payload;

        public IncomingMessage(String payload) {
            this.payload = payload;
        }
    }

    public static class OutgoingMessage {
        public final String payload;

        public OutgoingMessage(String payload) {
            this.payload = payload;
        }
    }


    private final static Logger logger = LoggerFactory.getLogger(NettyTesting.class);

    public static void main(String[] args) throws Exception {
        logger.debug("Debug message for bootstrapping");
        AMQPConnection
            .connect("localhost", 29468)
            .toFlowable()
            .flatMap(sourceConnection -> {
                Flowable<OutgoingMessage> outgoingMessages = sourceConnection
                    .consume("sourceQueue")
                    .map(incomingMessage -> new OutgoingMessage(incomingMessage.payload));

                return AMQPConnection
                    .connect("localhost", 29469)
                    .toFlowable()
                    .flatMap(targetConnection -> targetConnection.publish(outgoingMessages));
            })
            .retryWhen(linearBackoff())
            .subscribe(o -> logger.debug("Shoveled {}", o.payload));
    }

    public static Function<Flowable<Throwable>, Flowable<Object>> linearBackoff() {
        AtomicInteger retryCount = new AtomicInteger(0);
        return throwables -> throwables.delay(throwable -> {
            int retries = retryCount.incrementAndGet();
            return Flowable.timer(retries, TimeUnit.SECONDS);
        }).cast(Object.class);
    }

}
