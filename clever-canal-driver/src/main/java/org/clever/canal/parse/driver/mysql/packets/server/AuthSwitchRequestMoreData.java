package org.clever.canal.parse.driver.mysql.packets.server;


import org.clever.canal.parse.driver.mysql.packets.CommandPacket;
import org.clever.canal.parse.driver.mysql.utils.ByteHelper;

public class AuthSwitchRequestMoreData extends CommandPacket {

    public int status;
    public byte[] authData;

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }

    public byte[] toBytes() {
        return null;
    }
}
