package org.clever.canal.server.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.slf4j.MDC;

/**
 * Canal 数据同步功能处理
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 16:16 <br/>
 */
@Slf4j
public class SessionHandler2 extends SimpleChannelInboundHandler<CanalPacket.Packet> {
    private final CanalServerWithEmbedded embeddedServer;

    public SessionHandler2(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws Exception {
        log.info("[CanalServerWithNetty] message receives in session handler...");
        final long start = System.nanoTime();
        ClientIdentity clientIdentity;
        try {
            switch (msg.getType()) {
                // subscription(订阅)
                case SUBSCRIPTION:
                    CanalPacket.Sub sub = CanalPacket.Sub.parseFrom(msg.getBody());
                    if (StringUtils.isNotEmpty(sub.getDestination()) && StringUtils.isNotEmpty(sub.getClientId())) {
                        clientIdentity = new ClientIdentity(sub.getDestination(), Short.parseShort(sub.getClientId()), sub.getFilter());
                        MDC.put("destination", clientIdentity.getDestination());
//                        // 尝试启动，如果已经启动，忽略 TODO lzw
//                        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
//                            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
//                            if (!runningMonitor.isStart()) {
//                                runningMonitor.start();
//                            }
//                        }
                        embeddedServer.subscribe(clientIdentity);
                        // 返回
                    } else {
                        // 返回错误
                        // destination or clientId is null
                    }
                    break;
                // unsubscription(取消订阅)
                case UNSUBSCRIPTION:
                    CanalPacket.Unsub unsub = CanalPacket.Unsub.parseFrom(msg.getBody());
                    break;
                // Get(PullRequest)
                case GET:
                    CanalPacket.Get get = CanalPacket.Get.parseFrom(msg.getBody());
                    break;
                // client ack
                case CLIENT_ACK:
                    CanalPacket.ClientAck ack = CanalPacket.ClientAck.parseFrom(msg.getBody());
                    break;
                // client rollback
                case CLIENT_ROLLBACK:
                    CanalPacket.ClientRollback rollback = CanalPacket.ClientRollback.parseFrom(msg.getBody());
                    break;
                default:
                    // 返回错误
                    // packet type={} is NOT supported!
            }
        } finally {
            MDC.remove("destination");
        }
    }

    /**
     * 异常处理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[CanalServerWithNetty] something goes wrong with channel:{}, exception={}", ctx.channel(), cause);
        ctx.channel().close();
    }
}
