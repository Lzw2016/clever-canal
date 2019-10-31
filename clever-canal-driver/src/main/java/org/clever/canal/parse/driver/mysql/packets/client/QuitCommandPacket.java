package org.clever.canal.parse.driver.mysql.packets.client;

import org.clever.canal.parse.driver.mysql.packets.CommandPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * quit cmd
 */
@SuppressWarnings({"WeakerAccess"})
public class QuitCommandPacket extends CommandPacket {

    public static final byte[] QUIT = new byte[]{1, 0, 0, 0, 1};

    public QuitCommandPacket() {
        setCommand((byte) 0x01);
    }

    @Override
    public void fromBytes(byte[] data) {
    }

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        out.write(QUIT);
        return out.toByteArray();
    }
}
