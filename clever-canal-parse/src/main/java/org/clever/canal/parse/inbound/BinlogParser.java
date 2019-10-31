package org.clever.canal.parse.inbound;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.CanalEntry;

/**
 * 解析binlog的接口
 */
public interface BinlogParser<T> extends CanalLifeCycle {

    CanalEntry.Entry parse(T event, boolean isSeek) throws CanalParseException;

    void reset();
}
