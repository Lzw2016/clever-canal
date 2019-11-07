package org.clever.canal.server.netty.handler;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.server.netty.NettyServerConstant;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/07 09:40 <br/>
 */
@SuppressWarnings("WeakerAccess")
public class HandlerUtils {
    private static final String CHARSET = "UTF-8";
    public static final int Error_Code_400 = 400;
    public static final int Error_Code_401 = 401;
    public static final int Error_Code_402 = 402;

    public static CanalPacket.Handshake createHandshake(byte[] seed) {
        return CanalPacket.Handshake.newBuilder()
                .setCommunicationEncoding(CHARSET)
                .setSeeds(ByteString.copyFrom(seed))
                .setSupportedCompressions(NettyServerConstant.COMPRESSION)
                .build();
    }

    public static CanalPacket.Ack createAck(int errorCode, String errorMessage) {
        return CanalPacket.Ack.newBuilder().setErrorCode(errorCode).setErrorMessage(errorMessage).build();
    }

    public static CanalPacket.Packet createPacket(CanalPacket.PacketType packetType, GeneratedMessageV3 body) {
        return CanalPacket.Packet.newBuilder()
                // .setMagicNumber()
                .setVersion(NettyServerConstant.VERSION)
                .setType(packetType)
                .setCompression(NettyServerConstant.COMPRESSION)
                .setBody(body.toByteString())
                .build();
    }

    public static CanalPacket.Packet errorPacket(int errorCode, String errorMessage) {
        CanalPacket.Ack ack = createAck(errorCode, errorMessage);
        return createPacket(CanalPacket.PacketType.ACK, ack);
    }

    public static void write(Channel channel, GeneratedMessageV3 resMsg, ChannelFutureListener channelFutureListener) {
        if (resMsg == null) {
            return;
        }
        ChannelFuture channelFuture = channel.write(resMsg);
        if (channelFutureListener != null) {
            channelFuture.addListener(channelFutureListener);
        }
    }

    public static void write(Channel channel, GeneratedMessageV3 resMsg) {
        write(channel, resMsg, null);
    }

    public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListener) {
        if (body == null) {
            return;
        }
        ChannelFuture channelFuture = channel.write(body);
        if (channelFutureListener != null) {
            channelFuture.addListener(channelFutureListener);
        }
    }

    public static void writeAck(Channel channel, ChannelFutureListener channelFutureListener) {
        CanalPacket.Ack ack = CanalPacket.Ack.newBuilder().build();
        CanalPacket.Packet packet = createPacket(CanalPacket.PacketType.ACK, ack);
        write(channel, packet, channelFutureListener);
    }

    public static void writeAck(Channel channel) {
        writeAck(channel, null);
    }

    public static void writeError(Channel channel, int errorCode, String errorMessage) {
        CanalPacket.Packet packet = errorPacket(errorCode, errorMessage);
        write(channel, packet);
    }

    public static void writeError(Channel channel, String errorMessage) {
        writeError(channel, Error_Code_400, errorMessage);
    }

    public static void writeError(Channel channel, int errorCode, String errorMessage, ChannelFutureListener channelFutureListener) {
        CanalPacket.Packet packet = errorPacket(errorCode, errorMessage);
        write(channel, packet, channelFutureListener);
    }
}
