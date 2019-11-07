package org.clever.canal.server.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.clever.canal.protocol.CanalPacket;

/**
 * 连接握手处理
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 16:58 <br/>
 */
@Slf4j
public class HandshakeHandler extends ChannelInboundHandlerAdapter {
    /**
     * 连接成功
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 构造握手数据
        final byte[] seed = RandomUtils.nextBytes(8);
        CanalPacket.Handshake handshake = HandlerUtils.createHandshake(seed);
        CanalPacket.Packet packet = HandlerUtils.createPacket(CanalPacket.PacketType.HANDSHAKE, handshake);
        // 发送握手数据
        HandlerUtils.write(ctx.channel(), packet, future -> {
            ClientAuthenticationHandler clientAuthenticationHandler = ctx.pipeline().get(ClientAuthenticationHandler.class);
            clientAuthenticationHandler.setSeed(seed);
        });
    }
}
