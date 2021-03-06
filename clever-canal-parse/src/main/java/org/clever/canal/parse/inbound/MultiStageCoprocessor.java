package org.clever.canal.parse.inbound;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.LogEvent;

/**
 * 针对解析器提供一个多阶段协同的处理
 *
 * <pre>
 * 1. 网络接收 (单线程)
 * 2. 事件基本解析 (单线程，事件类型、DDL解析构造TableMeta、维护位点信息)
 * 3. 事件深度解析 (多线程, DML事件数据的完整解析)
 * 4. 投递到store (单线程)
 * </pre>
 */
public interface MultiStageCoprocessor extends CanalLifeCycle {
    /**
     * 网络数据投递
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean publish(LogBuffer buffer);

    boolean publish(LogEvent event);
}
