package org.clever.canal.instance.spring;

import org.clever.canal.common.alarm.CanalAlarmHandler;
import org.clever.canal.instance.core.AbstractCanalInstance;
import org.clever.canal.instance.core.CanalMQConfig;
import org.clever.canal.meta.CanalMetaManager;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.sink.CanalEventSink;
import org.clever.canal.store.CanalEventStore;
import org.clever.canal.store.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 基于spring容器启动canal实例，方便独立于manager启动
 */
@SuppressWarnings("unused")
public class CanalInstanceWithSpring extends AbstractCanalInstance {

    private static final Logger logger = LoggerFactory.getLogger(CanalInstanceWithSpring.class);

    public void start() {
        logger.info("start CannalInstance for {}-{} ", new Object[]{1, destination});
        super.start();
    }

    // ======== setter ========

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setEventParser(CanalEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventSink(CanalEventSink<List<CanalEntry.Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

    public void setMqConfig(CanalMQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }
}
