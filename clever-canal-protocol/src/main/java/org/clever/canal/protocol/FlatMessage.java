package org.clever.canal.protocol;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class FlatMessage implements Serializable {
    private static final long serialVersionUID = -3386650678735860050L;

    private long id;
    private String database;
    private String table;
    private List<String> pkNames;
    private Boolean isDdl;
    private String type;
    // binlog executeTime
    private Long es;
    // dml build timeStamp
    private Long ts;
    private String sql;
    private Map<String, Integer> sqlType;
    private Map<String, String> mysqlType;
    private List<Map<String, String>> data;
    private List<Map<String, String>> old;

    public FlatMessage() {
    }

    public FlatMessage(long id) {
        this.id = id;
    }
}
