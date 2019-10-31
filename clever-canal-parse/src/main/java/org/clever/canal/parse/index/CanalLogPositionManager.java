package org.clever.canal.parse.index;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.position.LogPosition;

public interface CanalLogPositionManager extends CanalLifeCycle {

    LogPosition getLatestIndexBy(String destination);

    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;
}
