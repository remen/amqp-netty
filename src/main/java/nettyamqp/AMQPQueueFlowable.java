package nettyamqp;

import io.netty.channel.ChannelHandlerContext;
import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicLong;

public class AMQPQueueFlowable extends Flowable<NettyTesting.IncomingMessage> {
    private final AMQPConnection amqpConnection;
    private final AMQPConnection.AMQPConnectionHandler amqpConnectionHandler;

    public AMQPQueueFlowable(AMQPConnection amqpConnection, AMQPConnection.AMQPConnectionHandler amqpConnectionHandler) {
        this.amqpConnection = amqpConnection;
        this.amqpConnectionHandler = amqpConnectionHandler;
    }

    @Override
    protected void subscribeActual(Subscriber<? super NettyTesting.IncomingMessage> subscriber) {
        AMQPQueueSubscription subscription = new AMQPQueueSubscription(subscriber, amqpConnection, amqpConnectionHandler);
        amqpConnection.onSubscribe(subscription);
        subscriber.onSubscribe(subscription);
    }

    public static class AMQPQueueSubscription extends AtomicLong implements Subscription {
        private final Subscriber<? super NettyTesting.IncomingMessage> subscriber;
        private final AMQPConnection amqpConnection;
        private final AMQPConnection.AMQPConnectionHandler amqpConnectionHandler;

        volatile private boolean cancelled = false;

        public AMQPQueueSubscription(Subscriber<? super NettyTesting.IncomingMessage> subscriber, AMQPConnection amqpConnection, AMQPConnection.AMQPConnectionHandler amqpConnectionHandler) {
            this.subscriber = subscriber;
            this.amqpConnection = amqpConnection;
            this.amqpConnectionHandler = amqpConnectionHandler;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.add(this, n) == 0L) {
                    this.amqpConnection.read();
                }
            }
        }

        @Override
        public void cancel() {
            this.cancelled = true;
        }

        public void onNext(NettyTesting.IncomingMessage incomingMessage) {

            subscriber.onNext(incomingMessage);
        }
    }
}
