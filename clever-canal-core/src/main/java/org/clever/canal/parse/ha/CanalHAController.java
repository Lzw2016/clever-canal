package org.clever.canal.parse.ha;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.parse.exception.CanalHAException;

/**
 * HA 控制器实现
 */
public interface CanalHAController extends CanalLifeCycle {

    void start() throws CanalHAException;

    void stop() throws CanalHAException;
}
