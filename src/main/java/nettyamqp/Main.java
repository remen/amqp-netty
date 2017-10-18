package nettyamqp;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class Main {
    public static class InMessage {
        private final String payload;

        public InMessage(String payload) {
            this.payload = payload;
        }

        public Acknowledgment<Void> ack() {
            return new Acknowledgment<>(Acknowledgment.AckType.ACK, payload, null);
        }

        public Acknowledgment<Void> reject() {
            return new Acknowledgment<>(Acknowledgment.AckType.REJECT, payload, null);
        }
    }

    public static class Acknowledgment<T> {
        public static enum AckType {
            ACK,
            REJECT,
            REQUEUE
        }

        private final AckType type;
        private final String tag;
        private final T context;


        private Acknowledgment(AckType type, String tag, T context) {
            this.type = type;
            this.tag = tag;
            this.context = context;
        }

        public <S> Acknowledgment<S> withContext(S context) {
            return new Acknowledgment<>(type, this.tag, context);
        }

        public AckType getType() {
            return type;
        }

        @Override
        public String toString() {
            return "Acknowledgment{" +
                "type=" + type +
                ", tag='" + tag + '\'' +
                ", context=" + context +
                '}';
        }
    }

    public interface AMQPMessageProcessor<T> extends Processor<InMessage, Acknowledgment<T>> {}

    public static class Acknowledger<T> {
        public Flowable<Acknowledgment<T>> acknowledge(Flowable<Acknowledgment<T>> acknowledgments) {
            return acknowledgments;
        }
    }

    public static class MessagesAndAcknowledger<T> {
        public final Flowable<InMessage> messages;
        public final Acknowledger<T> acknowledger;

        public MessagesAndAcknowledger(Flowable<InMessage> messages, Acknowledger<T> acknowledger) {
            this.messages = messages;
            this.acknowledger = acknowledger;
        }

        public static void main(String[] args) {
            Flowable.just("foobar").lift()


            MessagesAndAcknowledger<Object> bajs = bajs();
            bajs.messages.map(message -> message.ack()).compose(bajs.acknowledger::acknowledge)
        }
    }

    public static <T> MessagesAndAcknowledger<T> bajs() {
        return new MessagesAndAcknowledger<>(
            Flowable.just(new InMessage("Message 1")),
            new Acknowledger<>()
        );
    }

    public static <T> Publisher<Acknowledgment<T>> foobar(Processor<InMessage, Acknowledgment<T>> messageProcessor) {

//        messageProcessor.


        //        return Flowable
//            .just(new InMessage("Message 1"))
//            .subscribeWith(messageProcessor)
//        ;
//
    }

    public static interface MessageTransaction {

    }

    public static <T> Publisher<Acknowledgment<T>> consume(Function<Publisher<InMessage>, Publisher<Acknowledgment<T>>> transaction) {

        return Flowable
            .just(new InMessage("Message 1"), new InMessage("Message 2"))
            .compose(transaction::apply);
    }

//    public static <T> Publisher<Acknowledgment<T>> consume(Function<? super Publisher<InMessage>, ? extends Publisher<Acknowledgment<T>>> transaction) {
//        Flowable<InMessage> just = Flowable
//            .just(new InMessage("Message 1"), new InMessage("Message 2"));
//
//        Flowable<Acknowledgment<T>> compose = just.compose(upstream -> transaction.apply(upstream));
//        return compose;
//    }

    public static <T> Flowable<Acknowledgment<T>> rxConsume(Function<Flowable<InMessage>, Flowable<Acknowledgment<T>>> transaction) {



        return Flowable
            .just(new InMessage("Message 1"), new InMessage("Message 2"))
            .compose(transaction::apply);
    }

//    public static class MessagesAndAcknowledger {
//        public static void main(String[] args) {
//            Flowable
//                .fromPublisher(consume(messages ->
//                        Flowable
//                            .fromPublisher(messages)
//                            .map(message -> message
//                                .ack()
//                                .withContext(message.payload)
//                            )
//                    )
//                )
//                .doOnNext(stringAcknowledgment -> LOGGER.info("{}", stringAcknowledgment))
//                .blockingSubscribe();
//        }
//    }


    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("Hello world");
        Publisher<NettyTesting.IncomingMessage> localhost = AMQPConnection.consume("localhost", 29468, "/", "my-queue");
        Observable
            .fromPublisher(localhost)
            .retry()
            .subscribe(message -> {
                LOGGER.info("Got {}", message.payload);
            }, throwable -> {
                LOGGER.error("Got exception", throwable);
            });
    }
}
