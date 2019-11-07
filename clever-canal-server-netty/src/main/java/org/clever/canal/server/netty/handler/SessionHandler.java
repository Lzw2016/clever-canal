package org.clever.canal.server.netty.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.clever.canal.common.utils.CollectionUtils;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.Message;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.clever.canal.server.netty.NettyServerConstant;
import org.clever.canal.server.netty.listener.ChannelFutureAggregator;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Canal 数据同步功能处理
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 16:16 <br/>
 */
@Slf4j
public class SessionHandler extends SimpleChannelInboundHandler<CanalPacket.Packet> {
    private final CanalServerWithEmbedded embeddedServer;

    public SessionHandler(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, CanalPacket.Packet msg) {
        log.info("[CanalServerWithNetty] message receives in session handler...");
        final long start = System.nanoTime();
        try {
            switch (msg.getType()) {
                case SUBSCRIPTION:
                    // subscription(订阅)
                    subscription(ctx, msg);
                    break;
                case UNSUBSCRIPTION:
                    // unsubscription(取消订阅)
                    unsubscription(ctx, msg);
                    break;
                case GET:
                    // Get(PullRequest)
                    get(ctx, msg, start);
                    break;
                case CLIENT_ACK:
                    // client ack
                    clientAck(ctx, msg, start);
                    break;
                case CLIENT_ROLLBACK:
                    // client rollback
                    clientRollback(ctx, msg, start);
                    break;
                default:
                    // 返回错误
                    HandlerUtils.writeError(ctx.channel(), String.format("packet type=%s is NOT supported!", msg.getType()));
            }
        } catch (Throwable exception) {
            CanalPacket.Packet packet = HandlerUtils.errorPacket(
                    HandlerUtils.Error_Code_400,
                    String.format("something goes wrong with channel:%s, exception=%s", ctx.channel(), ExceptionUtils.getStackTrace(exception))
            );
            ChannelFutureAggregator channelFutureAggregator = new ChannelFutureAggregator(
                    ctx.channel().remoteAddress().toString(),
                    null,
                    msg.getType(),
                    packet.getSerializedSize(),
                    System.nanoTime() - start,
                    (short) HandlerUtils.Error_Code_400
            );
            HandlerUtils.write(ctx.channel(), packet, channelFutureAggregator);
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

    /**
     * 停止 CanalInstance
     */
    private void stopCanalInstanceIfNecessary(ClientIdentity clientIdentity) {
        List<ClientIdentity> clientIdentities = embeddedServer.listAllSubscribe(clientIdentity.getDestination());
        // noinspection StatementWithEmptyBody
        if (clientIdentities != null && clientIdentities.size() == 1 && clientIdentities.contains(clientIdentity)) {
//            TODO lzw
//            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
//            if (runningMonitor.isStart()) {
//                runningMonitor.release();
//            }
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private void subscription(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws InvalidProtocolBufferException {
        CanalPacket.Sub sub = CanalPacket.Sub.parseFrom(msg.getBody());
        if (StringUtils.isBlank(sub.getDestination()) || StringUtils.isBlank(sub.getClientId())) {
            HandlerUtils.writeError(ctx.channel(), HandlerUtils.Error_Code_401, "destination or clientId is null");
            return;
        }
        ClientIdentity clientIdentity = new ClientIdentity(sub.getDestination(), sub.getClientId(), sub.getFilter());
        MDC.put("destination", clientIdentity.getDestination());
//        // 尝试启动，如果已经启动，忽略 TODO lzw
//        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
//            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
//            if (!runningMonitor.isStart()) {
//                runningMonitor.start();
//            }
//        }
        // 订阅
        embeddedServer.subscribe(clientIdentity);
        // 返回
        HandlerUtils.writeAck(ctx.channel());
    }

    @SuppressWarnings("DuplicatedCode")
    private void unsubscription(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws InvalidProtocolBufferException {
        CanalPacket.Unsub unsub = CanalPacket.Unsub.parseFrom(msg.getBody());
        if (StringUtils.isBlank(unsub.getDestination()) || StringUtils.isBlank(unsub.getClientId())) {
            HandlerUtils.writeError(ctx.channel(), HandlerUtils.Error_Code_401, "destination or clientId is null");
            return;
        }
        ClientIdentity clientIdentity = new ClientIdentity(unsub.getDestination(), unsub.getClientId(), unsub.getFilter());
        MDC.put("destination", clientIdentity.getDestination());
        // 取消订阅
        embeddedServer.unsubscribe(clientIdentity);
        // 尝试关闭
        stopCanalInstanceIfNecessary(clientIdentity);
        // 返回
        HandlerUtils.writeAck(ctx.channel());
    }

    @SuppressWarnings("DuplicatedCode")
    private void get(ChannelHandlerContext ctx, CanalPacket.Packet msg, final long start) throws IOException {
        CanalPacket.Get get = CanalPacket.Get.parseFrom(msg.getBody());
        if (StringUtils.isBlank(get.getDestination()) || StringUtils.isBlank(get.getClientId())) {
            HandlerUtils.writeError(ctx.channel(), HandlerUtils.Error_Code_401, "destination or clientId is null");
            return;
        }
        ClientIdentity clientIdentity = new ClientIdentity(get.getDestination(), Short.parseShort(get.getClientId()));
        MDC.put("destination", clientIdentity.getDestination());
        Message message;
        // 是否是初始值
        if (get.getTimeout() == -1) {
            message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize());
        } else {
            TimeUnit unit = convertTimeUnit(get.getUnit());
            message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize(), get.getTimeout(), unit);
        }
        // 组装返回数据
        CanalPacket.Messages.Builder messageBuilder = CanalPacket.Messages.newBuilder();
        messageBuilder.setBatchId(message.getId());
        if (message.getId() != -1) {
            if (message.isRaw() && !CollectionUtils.isEmpty(message.getRawEntries())) {
                messageBuilder.addAllMessages(message.getRawEntries());
            } else if (!CollectionUtils.isEmpty(message.getEntries())) {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    messageBuilder.addMessages(entry.toByteString());
                }
            }
        }
        CanalPacket.Packet packet = CanalPacket.Packet.newBuilder()
                .setType(CanalPacket.PacketType.MESSAGES)
                .setVersion(NettyServerConstant.VERSION)
                .setBody(messageBuilder.build().toByteString())
                .build();
        ChannelFutureAggregator channelFutureAggregator = new ChannelFutureAggregator(
                get.getDestination(),
                get,
                msg.getType(),
                packet.getSerializedSize(),
                System.nanoTime() - start,
                message.getId() == -1
        );
        HandlerUtils.write(ctx.channel(), packet, channelFutureAggregator);
    }

    private void clientAck(ChannelHandlerContext ctx, CanalPacket.Packet msg, final long start) throws InvalidProtocolBufferException {
        CanalPacket.ClientAck ack = CanalPacket.ClientAck.parseFrom(msg.getBody());
        if (StringUtils.isBlank(ack.getDestination()) || StringUtils.isBlank(ack.getClientId())) {
            HandlerUtils.writeError(ctx.channel(), HandlerUtils.Error_Code_401, "destination or clientId is null");
            return;
        }
        MDC.put("destination", ack.getDestination());
        if (ack.getBatchId() == 0L) {
            CanalPacket.Packet packet = HandlerUtils.errorPacket(HandlerUtils.Error_Code_402, "batchId should assign value");
            ChannelFutureAggregator channelFutureAggregator = new ChannelFutureAggregator(
                    ack.getDestination(),
                    ack,
                    msg.getType(),
                    packet.getSerializedSize(),
                    System.nanoTime() - start,
                    (short) HandlerUtils.Error_Code_402
            );
            HandlerUtils.write(ctx.channel(), packet, channelFutureAggregator);
            return;
        }
        if (ack.getBatchId() == -1L) {
            // -1代表上一次get没有数据，直接忽略之
            return;
        }
        ClientIdentity clientIdentity = new ClientIdentity(ack.getDestination(), Short.parseShort(ack.getClientId()));
        embeddedServer.ack(clientIdentity, ack.getBatchId());
        ChannelFutureAggregator channelFutureAggregator = new ChannelFutureAggregator(
                ack.getDestination(),
                ack,
                msg.getType(),
                0,
                System.nanoTime() - start
        );
        channelFutureAggregator.operationComplete(null);
    }

    private void clientRollback(ChannelHandlerContext ctx, CanalPacket.Packet msg, final long start) throws InvalidProtocolBufferException {
        CanalPacket.ClientRollback rollback = CanalPacket.ClientRollback.parseFrom(msg.getBody());
        MDC.put("destination", rollback.getDestination());
        if (StringUtils.isBlank(rollback.getDestination()) || StringUtils.isBlank(rollback.getClientId())) {
            HandlerUtils.writeError(ctx.channel(), HandlerUtils.Error_Code_401, "destination or clientId is null");
            return;
        }
        ClientIdentity clientIdentity = new ClientIdentity(rollback.getDestination(), Short.parseShort(rollback.getClientId()));
        if (rollback.getBatchId() == 0L) {
            // 回滚所有批次
            embeddedServer.rollback(clientIdentity);
        } else {
            // 只回滚单个批次
            embeddedServer.rollback(clientIdentity, rollback.getBatchId());
        }
        ChannelFutureAggregator channelFutureAggregator = new ChannelFutureAggregator(rollback.getDestination(), rollback, msg.getType(), 0, System.nanoTime() - start);
        channelFutureAggregator.operationComplete(null);
    }

    private TimeUnit convertTimeUnit(int unit) {
        switch (unit) {
            case 0:
                return TimeUnit.NANOSECONDS;
            case 1:
                return TimeUnit.MICROSECONDS;
            case 2:
                // noinspection DuplicateBranchesInSwitch (压制警告)
                return TimeUnit.MILLISECONDS;
            case 3:
                return TimeUnit.SECONDS;
            case 4:
                return TimeUnit.MINUTES;
            case 5:
                return TimeUnit.HOURS;
            case 6:
                return TimeUnit.DAYS;
            default:
                return TimeUnit.MILLISECONDS;
        }
    }
}
