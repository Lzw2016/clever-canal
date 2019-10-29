package org.clever.canal.parse.index;

import com.google.common.collect.MapMaker;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.position.LogPosition;

import java.util.Map;
import java.util.Set;

@SuppressWarnings("WeakerAccess")
public class MemoryLogPositionManager extends AbstractLogPositionManager {

    private Map<String, LogPosition> positions;

    @Override
    public void start() {
        super.start();
        positions = new MapMaker().makeMap();
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
