package foobar;

import io.reactivex.Completable;
import io.reactivex.Flowable;

import java.util.concurrent.TimeUnit;

public class Foobar {
    public static void main(String[] args) {
        Flowable<Integer> input = Flowable
            .just(10,9,8,7,6,5,4,3,2,1)
            .publish()
            .autoConnect(2);

        Flowable<Completable> sendResult = input.compose(Foobar::send);

        input
            .zipWith(sendResult, (integer, completable) -> completable.toSingleDefault(integer))
            .flatMapSingle(completable -> completable)
            .blockingForEach(integer -> System.out.println(integer))
            ;
    }


    public static Flowable<Completable> send(Flowable<Integer> ints) {
        return ints.map(i -> Completable.timer(i * 100, TimeUnit.MILLISECONDS));
    }

//    private static final Logger LOGGER = LoggerFactory.getLogger(Foobar.class);
//
//    public static class Message {
//        private final String payload;
//
//        public Message(String payload) {
//            this.payload = payload;
//        }
//
//        public Completable ack() {
//            return Completable.fromAction(() -> LOGGER.debug("Acking " + this));
//        }
//
//        public Completable reject() {
//            return Completable.fromAction(() -> LOGGER.debug("Rejecting " + this));
//        }
//
//        public Completable requeue() {
//            return Completable.fromAction(() -> LOGGER.debug("Re-queueing " + this));
//        }
//
//        @Override
//        public String toString() {
//            return payload;
//        }
//    }
//
//    public static Flowable<Message> consumer(String exchange, String routingKey) {
//        return Flowable.fromArray(new Message("world"), new Message("fail"), new Message("hello"));
//    }
//
//    public static Completable send(String payload) {
//        Completable completable;
//        int seconds;
//
//        switch (payload) {
//            case "hello":
//                seconds = 1;
//                completable = Completable.complete();
//                break;
//            case "fail":
//                seconds = 2;
//                completable = Completable.error(new RuntimeException("fail"));
//                break;
//            case "world":
//                seconds = 3;
//                completable = Completable.complete();
//                break;
//            default:
//                return Completable.error(new RuntimeException("Unexpected message"));
//        }
//        return Completable
//            .timer(seconds, TimeUnit.SECONDS)
//            .andThen(completable);
//    }
//
//    public static Flowable<Message> messageProducer() {
//        return consumer("", "");
//    }
//
//    public static class Bar<Payload, Upstream, DownStream> {
//        private final Payload object;
//        private final FlowableTransformer<Upstream, DownStream> completableTransformer;
//
//        public Bar(Payload object, FlowableTransformer<Upstream, DownStream> completableTransformer) {
//            this.object = object;
//            this.completableTransformer = completableTransformer;
//        }
//    }
//
//    public static void main(String[] args) {
//        messageProducer()
//            .map(message ->
//                new Bar<>(message.payload, sendResult ->
//                    sendResult
//                        .ignoreElements()
//                        .andThen(message.ack())
//                        .onErrorResumeNext(throwable -> message.reject())
//                        .toFlowable()
//                )
//            )
//            .compose(bars ->
//                send(bars, 10)
//            ).blockingSubscribe();
////
////
////            .compose(messages -> send(messages, 10))
////            .doOnNext(messageResult -> {
////                if (messageResult.isSuccess()) {
////                    messageResult.message.ack();
////                } else {
////                    messageResult.message.reject();
////                }
////            })
////
////
////            .flatMapCompletable(message ->
////                send(message)
////                    .andThen(message.ack())
////                    .onErrorResumeNext(throwable -> message.reject())
////            )
////            .blockingAwait();
//    }
//
//    public static class MessageResult {
//        private final Message message;
//        private final Throwable throwable;
//
//        public MessageResult(Message message, Throwable throwable) {
//            this.message = message;
//            this.throwable = throwable;
//        }
//
//        public boolean isSuccess() {
//            return this.throwable == null;
//        }
//    }
//
//
//    public static <Upstream, Downstream> Flowable<Downstream> send(Flowable<Bar<String, Upstream, Downstream>> bars, int maxConcurrency) {
//        bars
//            .flatMap(bar -> {
//                    FlowableTransformer<Upstream, Downstream> completableTransformer = bar.completableTransformer;
//                    return send(bar.object)
//                        .toFlowable()
//                        .compose(completableTransformer);
//                },
//                false, maxConcurrency);
//    }
}
