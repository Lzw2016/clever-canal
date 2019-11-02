package org.clever.canal.parse.index;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.position.LogPosition;

/**
 * 管理binlog消费位置信息
 */
public interface CanalLogPositionManager extends CanalLifeCycle {
    /**
     * 读取消费位置信息
     *
     * @param destination 通道名称
     */
    LogPosition getLatestIndexBy(String destination);

    /**
     * 保存消费位置信息
     *
     * @param destination 通道名称
     * @param logPosition 消费位置信息
     */
    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;
}
