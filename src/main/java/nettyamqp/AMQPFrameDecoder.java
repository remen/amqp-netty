package nettyamqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.util.List;

import static com.sun.xml.internal.ws.spi.db.BindingContextFactory.LOGGER;
import static nettyamqp.AmqpClient.NAME;

public class AMQPFrameDecoder extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPFrameDecoder.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf)msg;
        buf.markReaderIndex();
        byte typeId = buf.readByte();
        int channel = buf.readUnsignedShort();
        int size = (int) buf.readUnsignedInt();
        ByteBuf payload = buf.readSlice(size);
        byte frameEndMarker = buf.readByte();
        DataInputStream dataInputStream = new DataInputStream(new ByteBufInputStream(payload));

        Object frame;
        switch (typeId) {
            case AMQP.FRAME_METHOD:
                frame = AMQImpl.readMethodFrom(dataInputStream);
                break;
            case AMQP.FRAME_HEADER:
                frame = AMQImpl.readContentHeaderFrom(dataInputStream);
                break;
            case AMQP.FRAME_BODY:
                buf.resetReaderIndex();
                frame = buf;
                break;
            default:
                throw new RuntimeException("Unknown something " + typeId);
        }
        LOGGER.debug("{}: Deserialized {}", ctx.channel().attr(NAME), frame);
        super.channelRead(ctx, frame);
    }

    public static class Body {
        public final ByteBuf payload;

        public Body(ByteBuf payload) {
            this.payload = payload;
        }
    }
}
