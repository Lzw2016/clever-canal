package org.clever.canal.server.netty.listener;

import com.google.protobuf.GeneratedMessageV3;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.server.netty.NettyServerConstant;
import org.clever.canal.server.netty.model.ClientRequestResult;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import static org.clever.canal.server.netty.CanalServerWithNettyProfiler.profiler;

@SuppressWarnings("unused")
public class ChannelFutureAggregator implements ChannelFutureListener {

    private ClientRequestResult result;

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency, boolean empty) {
        this(destination, request, type, amount, latency, empty, (short) 0);
    }

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency) {
        this(destination, request, type, amount, latency, false, (short) 0);
    }

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency, short errorCode) {
        this(destination, request, type, amount, latency, false, errorCode);
    }

    private ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency, boolean empty, short errorCode) {
        this.result = new ClientRequestResult.Builder()
                .destination(destination)
                .type(type)
                .request(request)
                .amount(amount + NettyServerConstant.HEADER_LENGTH)
                .latency(latency)
                .errorCode(errorCode)
                .empty(empty)
                .build();
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        // profiling after I/O operation
        if (future != null && future.getCause() != null) {
            result.setChannelError(future.getCause());
        }
        profiler().profiling(result);
    }
}
