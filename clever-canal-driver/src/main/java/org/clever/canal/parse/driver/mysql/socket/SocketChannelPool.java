package org.clever.canal.parse.driver.mysql.socket;

import org.apache.commons.lang3.StringUtils;

import java.net.SocketAddress;

@SuppressWarnings("unused")
public abstract class SocketChannelPool {

    public static SocketChannel open(SocketAddress address) throws Exception {
        String type = chooseSocketChannel();
        if ("netty".equalsIgnoreCase(type)) {
            return NettySocketChannelPool.open(address);
        } else {
            return BioSocketChannelPool.open(address);
        }
    }

    private static String chooseSocketChannel() {
        String socketChannel = System.getenv("canal.socketChannel");
        if (StringUtils.isEmpty(socketChannel)) {
            socketChannel = System.getProperty("canal.socketChannel");
        }
        if (StringUtils.isEmpty(socketChannel)) {
            socketChannel = "bio"; // bio or netty
        }
        return socketChannel;
    }
}
