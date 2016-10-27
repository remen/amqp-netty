package nettyamqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQImpl;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpClient {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpClient.class);

    public final static AttributeKey<String> NAME =AttributeKey.newInstance("name");

    public static void main(String[] args) throws Exception {
        int port = 5672;
        String host = "localhost";

        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        LOGGER.debug("Client connecting on host {} port {}", host, port);

        Channel channel = new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
//            .option(ChannelOption.AUTO_READ, false)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .attr(NAME, "inputChannel")
            .handler(new ChannelInitializer<SocketChannel>() {
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                        new LoggingHandler(),

                        new LengthFieldBasedFrameDecoder(131_072, 3, 4, 1, 0),
                        new AMQPFrameEncoder(),
                        new AMQPFrameDecoder(),

                        new AMQPConnectionHandler(),
                        new AMQPChannelHandler(),
                        new Shovel()
                    );
                }
            }).connect(host, port).sync().channel();
    }
}
