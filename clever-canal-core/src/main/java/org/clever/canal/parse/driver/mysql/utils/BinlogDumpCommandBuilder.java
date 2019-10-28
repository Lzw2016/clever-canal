package org.clever.canal.parse.driver.mysql.utils;

import org.apache.commons.lang3.StringUtils;
import org.clever.canal.parse.driver.mysql.packets.HeaderPacket;
import org.clever.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;

@SuppressWarnings("unused")
public class BinlogDumpCommandBuilder {
    public BinlogDumpCommandPacket build(String binglogFile, long position, long slaveId) {
        BinlogDumpCommandPacket command = new BinlogDumpCommandPacket();
        command.binlogPosition = position;
        if (!StringUtils.isEmpty(binglogFile)) {
            command.binlogFileName = binglogFile;
        }
        command.slaveServerId = slaveId;
        // end settings.
        return command;
    }

    public ChannelBuffer toChannelBuffer(BinlogDumpCommandPacket command) throws IOException {
        byte[] commandBytes = command.toBytes();
        byte[] headerBytes = assembleHeaderBytes(commandBytes.length);
        return ChannelBuffers.wrappedBuffer(headerBytes, commandBytes);
    }

    private byte[] assembleHeaderBytes(int length) {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(length);
        header.setPacketSequenceNumber((byte) 0x00);
        return header.toBytes();
    }
}
