package org.clever.canal.server.netty.handler;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.clever.canal.protocol.CanalPacket;

/**
 * 连接握手处理
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 16:58 <br/>
 */
@Slf4j
public class HandshakeInitializationHandler2 extends SimpleChannelInboundHandler<CanalPacket.Packet> {
    private static final int VERSION = 1;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CanalPacket.Packet msg) throws Exception {
        // 构造握手数据
        final byte[] seed = RandomUtils.nextBytes(8);
        CanalPacket.Handshake handshake = CanalPacket.Handshake.newBuilder()
                .setSeeds(ByteString.copyFrom(seed))
                .setSupportedCompressions(CanalPacket.Compression.NONE)
                .build();
        // 构造响应数据
        byte[] resData = CanalPacket.Packet.newBuilder()
                .setType(CanalPacket.PacketType.HANDSHAKE)
                .setVersion(VERSION)
                .setBody(handshake.toByteString())
                .build()
                .toByteArray();
        // 写入响应数据
        // NettyUtils.write(ctx.getChannel(), resData, ...);
    }
}
