package org.clever.canal.server.netty.handler;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.protocol.CanalPacket.ClientAuth;
import org.clever.canal.protocol.CanalPacket.Packet;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.clever.canal.server.netty.NettyUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.helpers.MessageFormatter;

import java.util.concurrent.TimeUnit;

/**
 * 客户端身份认证处理
 */
public class ClientAuthenticationHandler extends SimpleChannelHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    /**
     * 空闲时连接自动断开的超时时间
     */
    private final int defaultSubscriberDisconnectIdleTimeout = 60 * 60 * 1000;
    /**
     * CanalServer
     */
    private CanalServerWithEmbedded embeddedServer;
    /**
     * 认证数据签名(digest)
     */
    @Setter
    @Getter
    private byte[] seed;

    public ClientAuthenticationHandler(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        final Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
        final ClientAuth clientAuth = ClientAuth.parseFrom(packet.getBody());
        if (seed == null) {
            byte[] errorBytes = NettyUtils.errorPacket(400, MessageFormatter.format("auth failed for seed is null", clientAuth.getUsername()).getMessage());
            NettyUtils.write(ctx.getChannel(), errorBytes, null);
        }
        if (!embeddedServer.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
            byte[] errorBytes = NettyUtils.errorPacket(400, MessageFormatter.format("auth failed for user:{}", clientAuth.getUsername()).getMessage());
            NettyUtils.write(ctx.getChannel(), errorBytes, null);
        }
        // 如果存在订阅信息
        if (StringUtils.isNotEmpty(clientAuth.getDestination())
                && StringUtils.isNotEmpty(clientAuth.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(), Short.parseShort(clientAuth.getClientId()), clientAuth.getFilter());
            try {
                MDC.put("destination", clientIdentity.getDestination());
                embeddedServer.subscribe(clientIdentity);
//                        // 尝试启动，如果已经启动，忽略 TODO lzw
//                        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
//                            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
//                            if (!runningMonitor.isStart()) {
//                                runningMonitor.start();
//                            }
//                        }
            } finally {
                MDC.remove("destination");
            }
        }
        // 鉴权一次性，暂不统计
        NettyUtils.ack(ctx.getChannel(), new ChannelFutureListener() {

            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("remove unused channel handlers after authentication is done successfully.");
                ctx.getPipeline().remove(HandshakeInitializationHandler.class.getName());
                ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());

                int readTimeout = defaultSubscriberDisconnectIdleTimeout;
                int writeTimeout = defaultSubscriberDisconnectIdleTimeout;
                if (clientAuth.getNetReadTimeout() > 0) {
                    readTimeout = clientAuth.getNetReadTimeout();
                }
                if (clientAuth.getNetWriteTimeout() > 0) {
                    writeTimeout = clientAuth.getNetWriteTimeout();
                }
                // fix bug: soTimeout parameter's unit from connector is milliseconds.
                IdleStateHandler idleStateHandler = new IdleStateHandler(NettyUtils.hashedWheelTimer, readTimeout, writeTimeout, 0, TimeUnit.MILLISECONDS);
                ctx.getPipeline().addBefore(SessionHandler.class.getName(), IdleStateHandler.class.getName(), idleStateHandler);

                IdleStateAwareChannelHandler idleStateAwareChannelHandler = new IdleStateAwareChannelHandler() {
                    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {
                        logger.warn("channel:{} idle timeout exceeds, close channel to save server resources...", ctx.getChannel());
                        ctx.getChannel().close();
                    }
                };
                ctx.getPipeline().addBefore(SessionHandler.class.getName(), IdleStateAwareChannelHandler.class.getName(), idleStateAwareChannelHandler);
            }
        });
    }
}
