package org.clever.canal.parse.inbound;

import org.clever.canal.parse.driver.mysql.packets.GtIdSet;

import java.io.IOException;

/**
 * 通用的Erosa的链接接口, 用于一般化处理mysql/oracle的解析过程
 */
public interface ErosaConnection {

    void connect() throws IOException;

    void reconnect() throws IOException;

    void disconnect() throws IOException;

    /**
     * 用于快速数据查找,和dump的区别在于，seek会只给出部分的数据
     */
    void seek(String binlogFileName, Long binlogPosition, String gtid, SinkFunction func) throws IOException;

    void dump(String binlogFileName, Long binlogPosition, SinkFunction func) throws IOException;

    void dump(long timestamp, SinkFunction func) throws IOException;

    /**
     * 通过GTID同步binlog
     */
    void dump(GtIdSet gtidSet, SinkFunction func) throws IOException;

    // -------------

    void dump(String binlogFileName, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException;

    void dump(long timestamp, MultiStageCoprocessor coprocessor) throws IOException;

    void dump(GtIdSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException;

    ErosaConnection fork();

    long queryServerId() throws IOException;
}
