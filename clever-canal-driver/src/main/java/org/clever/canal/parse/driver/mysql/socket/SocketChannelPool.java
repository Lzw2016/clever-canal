package org.clever.canal.parse.driver.mysql.socket;

import java.net.SocketAddress;

public abstract class SocketChannelPool {

    public static SocketChannel open(SocketAddress address) throws Exception {
        return BioSocketChannelPool.open(address);
    }
}
