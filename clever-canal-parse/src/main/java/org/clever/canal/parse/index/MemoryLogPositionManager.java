package org.clever.canal.parse.index;

import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.position.LogPosition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理binlog消费位置信息(基于内存的实现)
 */
@SuppressWarnings("WeakerAccess")
public class MemoryLogPositionManager extends AbstractLogPositionManager {
    /**
     * 费位置信息 destination(通道名称) --> LogPosition(位置信息)
     */
    private Map<String, LogPosition> positions;

    @Override
    public void start() {
        super.start();
        positions = new ConcurrentHashMap<>();
    }

    @Override
    public void stop() {
        super.stop();
        positions.clear();
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        return positions.get(destination);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        positions.put(destination, logPosition);
    }

    public Set<String> destinations() {
        return positions.keySet();
    }
}
