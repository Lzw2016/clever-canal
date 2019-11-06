package org.clever.canal.server.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.slf4j.MDC;

/**
 * 客户端授权处理
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 17:10 <br/>
 */
public class ClientAuthenticationHandler2 extends SimpleChannelInboundHandler<CanalPacket.Packet> {
    /**
     * 空闲时连接自动断开的超时时间
     */
    private static final int defaultSubscriberDisconnectIdleTimeout = 60 * 60 * 1000;
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

    public ClientAuthenticationHandler2(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws Exception {
        final CanalPacket.ClientAuth clientAuth = CanalPacket.ClientAuth.parseFrom(msg.getBody());
        if (seed == null) {
            // 返回错误
            // auth failed for seed is null
            return;
        }
        // 客户端授权
        if (!embeddedServer.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
            // 授权失败返回错误
            // auth failed for user:{}
            return;
        }
        // 如果存在订阅信息
        if (StringUtils.isNotEmpty(clientAuth.getDestination()) && StringUtils.isNotEmpty(clientAuth.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(), Short.parseShort(clientAuth.getClientId()), clientAuth.getFilter());
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
        // 写入ACK返回消息
        // NettyUtils.ack(ctx.getChannel(), ...);
    }
}
