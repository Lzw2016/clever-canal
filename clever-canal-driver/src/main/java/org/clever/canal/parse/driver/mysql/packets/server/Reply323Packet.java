package org.clever.canal.parse.driver.mysql.packets.server;

import org.clever.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import org.clever.canal.parse.driver.mysql.utils.ByteHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Reply323Packet extends PacketWithHeaderPacket {

    public byte[] seed;

    public void fromBytes(byte[] data) {
    }

    public byte[] toBytes() throws IOException {
        if (seed == null) {
            return new byte[]{(byte) 0};
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteHelper.writeNullTerminated(seed, out);
            return out.toByteArray();
        }
    }
}
