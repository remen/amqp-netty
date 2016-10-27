package nettyamqp;

import com.rabbitmq.client.impl.Frame;
import com.rabbitmq.client.impl.Method;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;

class MethodAndChannel {
    private final Method method;
    private final int channel;

    MethodAndChannel(com.rabbitmq.client.Method method, int channel) {
        this.method = (Method)method;
        this.channel = channel;
    }

    private Frame toFrame() {
        try {
            return method.toFrame(channel);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    void writeTo(ByteBuf buf) throws IOException {
        toFrame().writeTo(new DataOutputStream(new ByteBufOutputStream(buf)));
    }

    @Override
    public String toString() {
        return "MethodAndChannel{" +
            "method=" + method +
            ", channel=" + channel +
            '}';
    }
}
