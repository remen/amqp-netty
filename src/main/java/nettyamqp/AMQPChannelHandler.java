package nettyamqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQImpl;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static nettyamqp.AmqpClient.NAME;

public class AMQPChannelHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPChannelHandler.class);

    private boolean connected = false;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(new MethodAndChannel(new AMQP.Channel.Open.Builder().build(), 1));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (connected) {
            ctx.fireChannelRead(msg);
            return;
        }
        if (msg instanceof AMQP.Channel.OpenOk) {
            LOGGER.debug("{}: Channel open", ctx.channel().attr(NAME));
            ctx.fireChannelActive();
            connected = true;
        }
    }
}
