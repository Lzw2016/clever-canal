package org.clever.canal.server.netty.handler;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * 解析对应的header信息
 */
public class FixedHeaderFrameDecoder extends ReplayingDecoder<Object> {

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        in.readBytes(in.readInt());
    }
}
