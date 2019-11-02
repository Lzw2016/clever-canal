package org.clever.canal.parse.index;

import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.position.LogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 管理binlog消费位置信息(实现二级管理模式)<br />
 * 一级管理器失败 就使用 二级管理器失败
 *
 * <pre>
 *     实现基于failover查找的机制完成meta的操作
 *     应用场景：比如针对内存buffer，出现HA切换，先尝试从内存buffer区中找到latest position，如果不存在才尝试找一下meta里消费的信息
 * </pre>
 */
@SuppressWarnings("unused")
public class FailBackLogPositionManager extends AbstractLogPositionManager {
    private final static Logger logger = LoggerFactory.getLogger(FailBackLogPositionManager.class);

    /**
     * 一级管理binlog消费位置信息
     */
    private final CanalLogPositionManager primary;
    /**
     * 二级管理binlog消费位置信息
     */
    private final CanalLogPositionManager secondary;

    /**
     * @param primary   一级管理binlog消费位置信息
     * @param secondary 二级管理binlog消费位置信息
     */
    public FailBackLogPositionManager(CanalLogPositionManager primary, CanalLogPositionManager secondary) {
        if (primary == null) {
            throw new NullPointerException("nul primary LogPositionManager");
        }
        if (secondary == null) {
            throw new NullPointerException("nul secondary LogPositionManager");
        }
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public void start() {
        super.start();
        if (!primary.isStart()) {
            primary.start();
        }
        if (!secondary.isStart()) {
            secondary.start();
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (secondary.isStart()) {
            secondary.stop();
        }
        if (primary.isStart()) {
            primary.stop();
        }
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = primary.getLatestIndexBy(destination);
        if (logPosition != null) {
            return logPosition;
        }
        return secondary.getLatestIndexBy(destination);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        try {
            primary.persistLogPosition(destination, logPosition);
        } catch (CanalParseException e) {
            logger.warn("persistLogPosition use primary log position manager exception. destination: {}, logPosition: {}", destination, logPosition, e);
            secondary.persistLogPosition(destination, logPosition);
        }
    }
}
