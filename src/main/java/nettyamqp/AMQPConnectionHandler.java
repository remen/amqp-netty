package nettyamqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQImpl.Connection.Open;
import com.rabbitmq.client.impl.AMQImpl.Connection.TuneOk;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static nettyamqp.AmqpClient.NAME;

public class AMQPConnectionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPConnectionHandler.class);

    private boolean connected = false;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(AMQPHeader.INSTANCE);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (connected) {
            super.channelRead(ctx, msg);
            return;
        }

        LOGGER.debug("{}: Connection handler received {}", ctx.channel().attr(NAME), msg);
        if (msg instanceof AMQP.Connection.Start) {
            String response = "\0" + "guest" + "\0" + "guest";

            ctx.writeAndFlush(
                new MethodAndChannel(
                    new AMQP.Connection.StartOk.Builder()
                        .response(response)
                        .build(),
                    0
                )
            );

        } else if (msg instanceof AMQP.Connection.Tune) {
//            AMQP.Connection.Tune tune = ((AMQP.Connection.Tune) msg);

            ctx.writeAndFlush(
                new MethodAndChannel(
                    new AMQP.Connection.TuneOk.Builder()
                        .channelMax(1)
                        .heartbeat(0)
                        .frameMax(131_072)
                        .build(),
                    0
                )
            );

            Open open = (Open) new AMQP.Connection.Open.Builder()
                .virtualHost("/")
                .build();
            ctx.writeAndFlush(new MethodAndChannel(open, 0));
        } else if (msg instanceof AMQP.Connection.OpenOk) {
            LOGGER.debug("{}: Connected!", ctx.channel().attr(NAME));
            connected = true;
            ctx.fireChannelActive();
        }
    }

    enum AMQPHeader {
        INSTANCE;

        private static final byte[] BYTES = new byte[]{
            65, 77, 81, 80, // AMQP in ASCII
            0,              // Separator
            0, 9, 1         // AMQP version
        };


        public final byte[] getBytes() {
            return BYTES;
        }
    }
}

