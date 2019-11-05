package org.clever.canal.common.alarm;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于log的alarm机制实现
 */
@SuppressWarnings("unused")
public class LogAlarmHandler extends AbstractCanalLifeCycle implements CanalAlarmHandler {
    private static final Logger logger = LoggerFactory.getLogger(LogAlarmHandler.class);

    public void sendAlarm(String destination, String msg) {
        logger.error("destination:{}[{}]", new Object[]{destination, msg});
    }
}
