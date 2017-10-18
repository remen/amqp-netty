package nettyamqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQContentHeader;
import com.rabbitmq.client.impl.AMQImpl;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static nettyamqp.AmqpClient.NAME;

public class Shovel extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Shovel.class);

    private Channel outboundChannel;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel inboundChannel = ctx.channel();

        // From now on, we will ask for reads whenever the output channel can
        // handle more.
        inboundChannel.config().setAutoRead(false);


        outboundChannel = new Bootstrap()
            .group(inboundChannel.eventLoop())
            .channel(inboundChannel.getClass())
            .option(ChannelOption.SO_KEEPALIVE, true)
            .attr(NAME, "outputChannel")
            .handler(new ChannelInitializer<SocketChannel>() {
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                        new LoggingHandler(),

                        new LengthFieldBasedFrameDecoder(131_072, 3, 4, 1, 0),
                        new AMQPFrameEncoder(),
                        new AMQPFrameDecoder(),

                        new AMQPConnectionHandler(),
                        new AMQPChannelHandler(),

                        new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx1) throws Exception {
                                LOGGER.debug("Shovel active");
                                MethodAndChannel consume = new MethodAndChannel(
                                    new AMQP.Basic.Consume.Builder()
                                        .noAck()
                                        .queue("input")
                                        .build(),
                                    1
                                );
                                inboundChannel.writeAndFlush(consume);
                                inboundChannel.read();
                            }
                        }
                    );
                }
            })
            .connect("localhost", 5672).channel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOGGER.debug("Shovel received {}", msg);
        if (msg instanceof AMQP.Basic.ConsumeOk) {
            LOGGER.debug("Consuming from input queue");
            ctx.channel().read();
            return;
        }

        if (outboundChannel.isActive()) {
            Object output = null;

            if (msg instanceof AMQP.Basic.Deliver) {
                AMQImpl.Basic.Publish publish = (AMQImpl.Basic.Publish) new AMQP.Basic.Publish.Builder()
                    .exchange("")
                    .routingKey("output")
                    .build();
                output = publish.toFrame(1);
            } else if (msg instanceof AMQContentHeader) {
                output = ((AMQContentHeader) msg).toFrame(1, ((AMQContentHeader) msg).getBodySize());
            } else if (msg instanceof ByteBuf) {
                output = msg;
            }
            outboundChannel.writeAndFlush(output).addListener((ChannelFuture f) -> {
                if (f.isSuccess()) {
                    ctx.channel().read();
                } else {
                    throw new RuntimeException("foobar");
                }
            });
        }
    }
}
