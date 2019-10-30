package org.clever.canal.parse.driver.mysql.packets.server;

import org.clever.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import org.clever.canal.parse.driver.mysql.utils.ByteHelper;

import java.io.IOException;

/**
 * <pre>
 * Type Of Result Packet       Hexadecimal Value Of First Byte (field_count)
 * ---------------------------------------------------------------------------
 * Result Set Packet           1-250 (first byte of Length-Coded Binary)
 * </pre>
 * <p>
 * The sequence of result set packet:
 *
 * <pre>
 *   (Result Set Header Packet)  the number of columns
 *   (Field Packets)             column descriptors
 *   (EOF Packet)                marker: end of Field Packets
 *   (Row Data Packets)          row contents
 * (EOF Packet)                marker: end of Data Packets
 *
 * <pre>
 *
 * @author fujohnwang
 */
@SuppressWarnings("unused")
public class ResultSetHeaderPacket extends PacketWithHeaderPacket {

    private long columnCount;
    private long extra;

    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        byte[] colCountBytes = ByteHelper.readBinaryCodedLengthBytes(data, index);
        columnCount = ByteHelper.readLengthCodedBinary(colCountBytes, index);
        index += colCountBytes.length;
        if (index < data.length - 1) {
            extra = ByteHelper.readLengthCodedBinary(data, index);
        }
    }

    public byte[] toBytes() {
        return null;
    }

    public long getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(long columnCount) {
        this.columnCount = columnCount;
    }

    public long getExtra() {
        return extra;
    }

    public void setExtra(long extra) {
        this.extra = extra;
    }

    public String toString() {
        return "ResultSetHeaderPacket [columnCount=" + columnCount + ", extra=" + extra + "]";
    }
}
