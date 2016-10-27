package nettyamqp;

import com.rabbitmq.client.impl.Method;
import com.rabbitmq.client.impl.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;

import static nettyamqp.AmqpClient.NAME;

public class AMQPFrameEncoder extends ChannelOutboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPFrameEncoder.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Frame) {
            LOGGER.debug("{}: Sending {}", ctx.channel().attr(NAME), msg);
            ByteBuf out = ctx.alloc().ioBuffer();
            ((Frame) msg).writeTo(new DataOutputStream(new ByteBufOutputStream(out)));
            ctx.write(out).addListener(f -> promise.setSuccess());
        } else if (msg instanceof AMQPConnectionHandler.AMQPHeader) {
            LOGGER.debug("{}: Sending HEADER", ctx.channel().attr(NAME));
            ByteBuf out = ctx.alloc().ioBuffer(((AMQPConnectionHandler.AMQPHeader) msg).getBytes().length);
            out.writeBytes(((AMQPConnectionHandler.AMQPHeader) msg).getBytes());
            ctx.write(out).addListener(f -> promise.setSuccess());
        } else if (msg instanceof ByteBuf) {
            LOGGER.debug("{}: Sending {}", ctx.channel().attr(NAME), msg);
            ctx.write(msg).addListener(f -> promise.setSuccess());
        } else if (msg instanceof MethodAndChannel) {
            LOGGER.debug("{}: Sending {}", ctx.channel().attr(NAME), msg);

            ByteBuf out = ctx.alloc().ioBuffer();
            ((MethodAndChannel) msg).writeTo(out);
            ctx.write(out).addListener(f -> promise.setSuccess());
        }
    }
}
