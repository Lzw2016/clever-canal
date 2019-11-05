package org.clever.canal.instance.core;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.common.alarm.CanalAlarmHandler;
import org.clever.canal.meta.CanalMetaManager;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.sink.CanalEventSink;
import org.clever.canal.store.CanalEventStore;

/**
 * 代表单个canal实例，比如一个destination会独立一个实例
 */
@SuppressWarnings({"unused"})
public interface CanalInstance extends CanalLifeCycle {

    String getDestination();

    CanalEventParser getEventParser();

    CanalEventSink getEventSink();

    CanalEventStore getEventStore();

    CanalMetaManager getMetaManager();

    CanalAlarmHandler getAlarmHandler();

    /**
     * 客户端发生订阅/取消订阅行为
     */
    @SuppressWarnings("UnusedReturnValue")
    boolean subscribeChange(ClientIdentity identity);

    CanalMQConfig getMqConfig();
}
