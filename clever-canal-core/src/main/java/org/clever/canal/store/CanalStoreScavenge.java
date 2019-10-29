package org.clever.canal.store;

import org.clever.canal.protocol.position.Position;

/**
 * store空间回收机制，信息采集以及控制何时调用{@linkplain CanalEventStore}.cleanUtil()接口
 */
@SuppressWarnings("unused")
public interface CanalStoreScavenge {

    /**
     * 清理position之前的数据
     */
    void cleanUntil(Position position) throws CanalStoreException;

    /**
     * 删除所有的数据
     */
    void cleanAll() throws CanalStoreException;
}
