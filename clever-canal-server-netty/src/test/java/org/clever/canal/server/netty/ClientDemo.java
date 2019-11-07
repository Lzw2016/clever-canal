package org.clever.canal.server.netty;

import com.google.protobuf.ByteString;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.protocol.SecurityUtil;
import org.clever.canal.server.netty.handler.HandlerUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/07 14:53 <br/>
 */
@Slf4j
public class ClientDemo {
    private static final String host = "127.0.0.1";
    private static final int port = 8000;
    private static final String username = "lizw";
    private static final String password = "123";

    private static final String destination = "test";
    private static final short clientId = 1001;
    private static final String filter = "";

    private static AtomicLong readCount = new AtomicLong(0);

    @Test
    public void client() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap client = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @SuppressWarnings("SpellCheckingInspection")
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // -------------------------- 解码和编码，应和客户端一致 (传输的协议 Protobuf) -------------------------- //
                            // 用于decode前解决半包和粘包问题（利用包头中的包含数组长度来识别半包粘包）
                            pipeline.addLast("ProtobufVarint32FrameDecoder", new ProtobufVarint32FrameDecoder());
                            // 反序列化指定的 Protobuf 字节数组为 Protobuf 类型
                            pipeline.addLast("ProtobufDecoder", new ProtobufDecoder(CanalPacket.Packet.getDefaultInstance()));
                            // 用于在序列化的字节数组前加上一个简单的包头，只包含序列化的字节长度
                            pipeline.addLast("ProtobufVarint32LengthFieldPrepender", new ProtobufVarint32LengthFieldPrepender());
                            // 用于对 Protobuf 类型序列化
                            pipeline.addLast("ProtobufEncoder", new ProtobufEncoder());
                            // -------------------------- CanalClient业务逻辑 -------------------------- //
                            pipeline.addLast("handler", new ClientChannelHandler());
                        }
                    });
            Channel channel = client.connect(host, port).sync().channel();
            Thread.sleep(1000 * 100);
            channel.close();
        } finally {
            group.shutdownGracefully();
        }
    }

    public static class ClientChannelHandler extends SimpleChannelInboundHandler<CanalPacket.Packet> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws Exception {
            readCount.incrementAndGet();
            log.info("读取数据次数{} | {}", readCount.get(), msg.getType());
            switch (msg.getType()) {
                case HANDSHAKE:
                    if (readCount.get() != 1) {
                        log.warn("握手数据不是第一次收到的消息!");
                    }
                    // 连接握手
                    CanalPacket.Handshake handshake = CanalPacket.Handshake.parseFrom(msg.getBody());
                    String newPassword = SecurityUtil.byte2HexStr(SecurityUtil.scramble411(password.getBytes(), handshake.getSeeds().toByteArray()));
                    CanalPacket.ClientAuth clientAuth = CanalPacket.ClientAuth.newBuilder()
                            .setUsername(username)
                            .setPassword(ByteString.copyFromUtf8(newPassword))
                            .setNetReadTimeout(10)
                            .setNetWriteTimeout(10)
                            .build();
                    HandlerUtils.write(ctx.channel(), HandlerUtils.createPacket(CanalPacket.PacketType.CLIENT_AUTHENTICATION, clientAuth));
                    break;
                case ACK:
                    CanalPacket.Ack ack = CanalPacket.Ack.parseFrom(msg.getBody());
                    if (ack.getErrorCode() > 0) {
                        log.warn("[Error] {}:{}", ack.getErrorCode(), ack.getErrorMessage());
                    }
                    if (readCount.get() == 2) {
                        // 开始订阅
                        CanalPacket.Sub sub = CanalPacket.Sub.newBuilder()
                                .setDestination(destination)
                                .setClientId(String.valueOf(clientId))
                                .setFilter(filter)
                                .build();
                        HandlerUtils.write(ctx.channel(), HandlerUtils.createPacket(CanalPacket.PacketType.SUBSCRIPTION, sub));
                    } else {
                        log.info("[ACK] 开始GET数据 {}:{}", ack.getErrorCode(), ack.getErrorMessage());
                        // 获取数据
                        CanalPacket.Get get = CanalPacket.Get.newBuilder()
                                .setAutoAck(false)
                                .setDestination(destination)
                                .setClientId(String.valueOf(clientId))
                                .setFetchSize(1)
                                .setTimeout(10)
                                .setUnit(5)
                                .build();
                        HandlerUtils.write(ctx.channel(), HandlerUtils.createPacket(CanalPacket.PacketType.GET, get));
                    }
                    break;
                case MESSAGES:
                    CanalPacket.Messages messages = CanalPacket.Messages.parseFrom(msg.getBody());
                    log.info("[Messages] BatchId = {}", messages.getBatchId());
                    CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(msg.getBody());
                    // ACK
                    CanalPacket.ClientAck clientAck = CanalPacket.ClientAck.newBuilder()
                            .setDestination(destination)
                            .setClientId(String.valueOf(clientId))
                            .setBatchId(messages.getBatchId())
                            .build();
                    HandlerUtils.write(ctx.channel(), HandlerUtils.createPacket(CanalPacket.PacketType.ACK, clientAck));
                    break;
                default:
                    log.warn("[default] {}", msg.getType());
            }
        }
    }
}
