package nettyamqp;

import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;

public class MoreFoobar implements Publisher<String> {
    public static void main(String[] args) {
        Flowable
            .range(0, 100_000)
            .subscribe(new Subscriber<Integer>() {
                public Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    this.subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(Integer integer) {
                    System.out.println(integer);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onComplete() {

                }
            });
    }

    @Override
    public void subscribe(Subscriber<? super String> subscriber) {
        subscriber.onSubscribe(
            new AMQPQueueSubscription(subscriber)
        );
    }

    public static class AMQPQueueSubscription implements Subscription {
        private final AtomicBoolean isCancelled = new AtomicBoolean(false);

        private final Subscriber<? super String> subscriber;

        public AMQPQueueSubscription(Subscriber<? super String> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            for(int i = 0; i < n; i++) {
                if (isCancelled.get()) {
                    return;
                }
                subscriber.onNext(Long.toString(n));
            }
        }

        @Override
        public void cancel() {
            isCancelled.lazySet(true);
        }
    }
}

