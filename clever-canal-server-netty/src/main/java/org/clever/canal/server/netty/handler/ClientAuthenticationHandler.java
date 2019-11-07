package org.clever.canal.server.netty.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.slf4j.MDC;

import java.util.concurrent.TimeUnit;

/**
 * 客户端授权处理
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 17:10 <br/>
 */
@Slf4j
public class ClientAuthenticationHandler extends SimpleChannelInboundHandler<CanalPacket.Packet> {
    /**
     * 空闲时连接自动断开的超时时间 (单位:秒)
     */
    private static final int defaultSubscriberDisconnectIdleTimeout = 60 * 60;
    /**
     * CanalServer
     */
    private final CanalServerWithEmbedded embeddedServer;
    /**
     * 认证数据签名(digest)
     */
    @Setter
    @Getter
    private byte[] seed;

    public ClientAuthenticationHandler(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws Exception {
        final CanalPacket.ClientAuth clientAuth = CanalPacket.ClientAuth.parseFrom(msg.getBody());
        if (seed == null) {
            HandlerUtils.writeError(ctx.channel(), String.format("[%s] auth failed for seed is null", clientAuth.getUsername()));
            return;
        }
        // 客户端授权
        if (!embeddedServer.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
            HandlerUtils.writeError(ctx.channel(), String.format("auth failed for user:%s", clientAuth.getUsername()));
            return;
        }
        // 如果存在订阅信息
        if (StringUtils.isNotBlank(clientAuth.getDestination()) && StringUtils.isNotBlank(clientAuth.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(), clientAuth.getClientId(), clientAuth.getFilter());
            try {
                MDC.put("destination", clientIdentity.getDestination());
                // 客户端注册订阅
                embeddedServer.subscribe(clientIdentity);
//                // 尝试启动，如果已经启动，忽略 TODO lzw
//                if (!embeddedServer.isStart(clientIdentity.getDestination())) {
//                    ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
//                    if (!runningMonitor.isStart()) {
//                        runningMonitor.start();
//                    }
//                }
            } finally {
                MDC.remove("destination");
            }
        }
        // 鉴权一次性，暂不统计
        HandlerUtils.writeAck(ctx.channel(), future -> {
            log.info("[CanalServerWithNetty] remove unused channel handlers after authentication is done successfully.");
            ctx.pipeline().remove(HandshakeHandler.class);
            ctx.pipeline().remove(ClientAuthenticationHandler.class);

            // 设置读写空闲超时时间
            int readTimeout = defaultSubscriberDisconnectIdleTimeout;
            int writeTimeout = defaultSubscriberDisconnectIdleTimeout;
            if (clientAuth.getNetReadTimeout() > 0) {
                readTimeout = clientAuth.getNetReadTimeout();
            }
            if (clientAuth.getNetWriteTimeout() > 0) {
                writeTimeout = clientAuth.getNetWriteTimeout();
            }
            IdleStateHandler idleStateHandler = new IdleStateHandler(readTimeout, writeTimeout, 0, TimeUnit.SECONDS);
            ctx.pipeline().addBefore("SessionHandler", "IdleStateHandler", idleStateHandler);
            ChannelHandler heartBeatServerHandler = new ChannelInboundHandlerAdapter() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                    log.warn("[CanalServerWithNetty] channel:{} idle timeout exceeds, close channel to save server resources...", ctx.channel());
                    ctx.channel().close();
                }
            };
            ctx.pipeline().addBefore("SessionHandler", "HeartBeatServerHandler", heartBeatServerHandler);
        });
    }
}
