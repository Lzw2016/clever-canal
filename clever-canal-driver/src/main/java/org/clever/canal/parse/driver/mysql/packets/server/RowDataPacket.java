package org.clever.canal.parse.driver.mysql.packets.server;

import org.clever.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import org.clever.canal.parse.driver.mysql.utils.LengthCodedStringReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"unused"})
public class RowDataPacket extends PacketWithHeaderPacket {

    private List<String> columns = new ArrayList<>();

    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        LengthCodedStringReader reader = new LengthCodedStringReader(null, index);
        do {
            getColumns().add(reader.readLengthCodedString(data));
        } while (reader.getIndex() < data.length);
    }

    public byte[] toBytes() {
        return null;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String toString() {
        return "RowDataPacket [columns=" + columns + "]";
    }
}
