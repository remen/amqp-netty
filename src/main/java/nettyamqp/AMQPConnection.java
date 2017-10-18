package nettyamqp;

import com.rabbitmq.client.impl.AMQConnection;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleEmitter;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static nettyamqp.AmqpClient.NAME;

public class AMQPConnection {
    public static final NioEventLoopGroup GROUP = new NioEventLoopGroup();

    private final ChannelHandlerContext ctx;
    private final AMQPConnectionHandler amqpConnectionHandler;
    private AMQPQueueFlowable.AMQPQueueSubscription subscription;

    public AMQPConnection(ChannelHandlerContext ctx, AMQPConnectionHandler amqpConnectionHandler) {
        this.ctx = ctx;
        this.amqpConnectionHandler = amqpConnectionHandler;
    }

    public Flowable<NettyTesting.IncomingMessage> consume(String queue) {
        return new AMQPQueueFlowable(this, this.amqpConnectionHandler);
    }

    public Flowable<NettyTesting.OutgoingMessage> publish(Flowable<NettyTesting.OutgoingMessage> outgoingMessages) {
        return outgoingMessages
            .observeOn(Schedulers.from(ctx.channel().eventLoop()))
            .flatMap(outgoingMessage ->
                Single
                    .create((SingleEmitter<NettyTesting.OutgoingMessage> emitter) -> {
                        ByteBuf msg = ctx.alloc().ioBuffer().writeBytes(outgoingMessage.payload.getBytes(StandardCharsets.UTF_8));
                        ctx.writeAndFlush(msg).addListener(future -> {
                            if (future.isSuccess()) {
                                emitter.onSuccess(outgoingMessage);
                            } else {
                                emitter.onError(future.cause());
                            }
                        });
                    })
                    .toFlowable());
    }

    private void emit(String s) {
        subscription.onNext(new NettyTesting.IncomingMessage(s));
    }

    public void read() {
        ctx.channel().read(); // Read at least once, the rest of the logic is in channelRead
    }

    public void onSubscribe(AMQPQueueFlowable.AMQPQueueSubscription subscription) {
        this.subscription = subscription;
    }





    public static class AMQPConnectionHandler extends ChannelDuplexHandler {
        private AMQPConnection connection;



        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            String s = byteBuf.toString(StandardCharsets.UTF_8);
            connection.emit(s);
            byteBuf.release();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            connection = new AMQPConnection(ctx, this);
        }

    }

    public static class AMQPMessagePublisher implements Publisher<NettyTesting.IncomingMessage> {

        private final String host;
        private final int port;
        private final String vhost;
        private final String queue;

        public AMQPMessagePublisher(String host, int port, String vhost, String queue) {
            this.host = host;
            this.port = port;
            this.vhost = vhost;
            this.queue = queue;
        }

        @Override
        public void subscribe(Subscriber<? super NettyTesting.IncomingMessage> subscriber) {
            new Bootstrap()
                .group(GROUP)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, false)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .attr(NAME, "inputChannel")
                .remoteAddress(host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                            new LoggingHandler(),
                            new QueueSubscriptionHandler(subscriber)
                        );

                    }
                })
                .connect();
        }

        public static class QueueSubscriptionHandler extends ChannelDuplexHandler implements Subscription {
            private final Subscriber<? super NettyTesting.IncomingMessage> subscriber;
            private final AtomicLong demand = new AtomicLong(0);

            volatile private Channel channel;
            volatile private boolean active;

            public QueueSubscriptionHandler(Subscriber<? super NettyTesting.IncomingMessage> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                this.subscriber.onError(new RuntimeException("Disconnected"));
            }

            @Override
            public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
                this.channel = ctx.channel();
                this.subscriber.onSubscribe(this);
            }

            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                this.active = true;
                if (demand.get() > 0) {
                    ctx.channel().read();
                }
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                subscriber.onNext(new NettyTesting.IncomingMessage("Message"));
                if (demand.decrementAndGet() > 0) {
                    ctx.read();
                }
            }

            @Override
            public void request(long n) {
                if (demand.getAndAdd(n) == 0 && active) {
                    this.channel.eventLoop().execute(() ->
                        this.channel.read()
                    );
                }
            }

            @Override
            public void cancel() {

            }
        }
    }


    public static Publisher<NettyTesting.IncomingMessage> consume(
        String host,
        int port,
        String vhost,
        String queue) {

        return new AMQPMessagePublisher(host, port, vhost, queue);
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(AMQPConnection.class);
    public static void main(String[] args) {
        System.out.println("Hello?");
        LOGGER.info("Initializing log (sigh)");
    }

    public static Single<AMQPConnection> connect(String host, int port) {
        return Single.create(onSubscribe -> {
        });
    }
}
