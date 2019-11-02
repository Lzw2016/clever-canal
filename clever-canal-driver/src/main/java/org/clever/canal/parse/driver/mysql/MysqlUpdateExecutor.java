package org.clever.canal.parse.driver.mysql;

import org.clever.canal.parse.driver.mysql.packets.client.QueryCommandPacket;
import org.clever.canal.parse.driver.mysql.packets.server.ErrorPacket;
import org.clever.canal.parse.driver.mysql.packets.server.OKPacket;
import org.clever.canal.parse.driver.mysql.utils.PacketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 默认输出的数据编码为UTF-8，如有需要请正确转码
 */
public class MysqlUpdateExecutor {
    private static final Logger logger = LoggerFactory.getLogger(MysqlUpdateExecutor.class);

    private MysqlConnector connector;

    public MysqlUpdateExecutor(MysqlConnector connector) throws IOException {
        if (!connector.isConnected()) {
            throw new IOException("should execute connector.connect() first");
        }
        this.connector = connector;
    }

    public OKPacket update(String updateString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(updateString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.writeBody(connector.getChannel(), bodyBytes);
        logger.debug("read update result...");
        byte[] body = PacketManager.readBytes(connector.getChannel(), PacketManager.readHeader(connector.getChannel(), 4).getPacketBodyLength());
        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + updateString);
        }
        OKPacket packet = new OKPacket();
        packet.fromBytes(body);
        return packet;
    }
}
