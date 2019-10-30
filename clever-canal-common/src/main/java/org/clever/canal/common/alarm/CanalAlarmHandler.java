package org.clever.canal.common.alarm;

import org.clever.canal.common.CanalLifeCycle;

/**
 * canal报警处理机制
 */
public interface CanalAlarmHandler extends CanalLifeCycle {

    /**
     * 发送对应destination的报警
     */
    void sendAlarm(String destination, String msg);
}
