package org.clever.canal.instance.core;

import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.alarm.CanalAlarmHandler;
import org.clever.canal.filter.aviater.AviaterRegexFilter;
import org.clever.canal.meta.CanalMetaManager;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.ha.CanalHAController;
import org.clever.canal.parse.ha.HeartBeatHAController;
import org.clever.canal.parse.inbound.AbstractEventParser;
import org.clever.canal.parse.inbound.group.GroupEventParser;
import org.clever.canal.parse.inbound.mysql.MysqlEventParser;
import org.clever.canal.parse.index.CanalLogPositionManager;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.sink.CanalEventSink;
import org.clever.canal.store.CanalEventStore;
import org.clever.canal.store.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * canal实例抽象实现类
 */
@SuppressWarnings({"WeakerAccess"})
public class AbstractCanalInstance extends AbstractCanalLifeCycle implements CanalInstance {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCanalInstance.class);

    /**
     * 和manager交互唯一标示
     */
    protected Long canalId;
    /**
     * 队列名字(通道名称)
     */
    protected String destination;
    /**
     * 有序队列
     */
    protected CanalEventStore<Event> eventStore;
    /**
     * 解析对应的数据信息
     */
    protected CanalEventParser eventParser;
    /**
     * 链接parse和store的桥接器
     */
    protected CanalEventSink<List<CanalEntry.Entry>> eventSink;
    /**
     * 消费信息管理器
     */
    protected CanalMetaManager metaManager;
    /**
     * alarm报警机制
     */
    protected CanalAlarmHandler alarmHandler;
    /**
     * mq的配置
     */
    protected CanalMQConfig mqConfig;

    /**
     * 客户端发生订阅/取消订阅行为
     */
    @Override
    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            logger.info("[{}-{}] subscribe filter change to {}", canalId, destination, identity.getFilter());
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());

            boolean isGroup = (eventParser instanceof GroupEventParser);
            if (isGroup) {
                // 处理group的模式
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                // 需要遍历启动
                for (CanalEventParser singleEventParser : eventParsers) {
                    if (singleEventParser instanceof AbstractEventParser) {
                        ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                    }
                }
            } else {
                if (eventParser instanceof AbstractEventParser) {
                    ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
                }
            }
        }
        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        return true;
    }

    /**
     * 启动 CanalInstance
     */
    @Override
    public void start() {
        logger.info("[{}-{}] Starting CanalInstance...", canalId, destination);
        super.start();
        if (!metaManager.isStart()) {
            metaManager.start();
        }
        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }
        if (!eventStore.isStart()) {
            eventStore.start();
        }
        if (!eventSink.isStart()) {
            eventSink.start();
        }
        if (!eventParser.isStart()) {
            beforeStartEventParser(eventParser);
            eventParser.start();
            afterStartEventParser(eventParser);
        }
        logger.info("[{}-{}] Started Successful!", canalId, destination);
    }

    /**
     * 停止 CanalInstance
     */
    @Override
    public void stop() {
        logger.info("[{}-{}] Stopping CanalInstance...", canalId, destination);
        super.stop();
        if (eventParser.isStart()) {
            beforeStopEventParser(eventParser);
            eventParser.stop();
            afterStopEventParser(eventParser);
        }
        if (eventSink.isStart()) {
            eventSink.stop();
        }
        if (eventStore.isStart()) {
            eventStore.stop();
        }
        if (metaManager.isStart()) {
            metaManager.stop();
        }
        if (alarmHandler.isStart()) {
            alarmHandler.stop();
        }
        logger.info("[{}-{}] Stopped Successful!", canalId, destination);
    }

    @Override
    public String getDestination() {
        return destination;
    }

    @Override
    public CanalEventParser getEventParser() {
        return eventParser;
    }

    @Override
    public CanalEventSink<List<CanalEntry.Entry>> getEventSink() {
        return eventSink;
    }

    @Override
    public CanalEventStore<Event> getEventStore() {
        return eventStore;
    }

    @Override
    public CanalMetaManager getMetaManager() {
        return metaManager;
    }

    @Override
    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    @Override
    public CanalMQConfig getMqConfig() {
        return mqConfig;
    }

    /**
     * 启动binlog解析之前
     */
    protected void beforeStartEventParser(CanalEventParser eventParser) {
        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {
                // 需要遍历启动
                startEventParserInternal(singleEventParser, true);
            }
        } else {
            startEventParserInternal(eventParser, false);
        }
    }

    /**
     * 启动binlog解析之后
     */
    @SuppressWarnings("unused")
    protected void afterStartEventParser(CanalEventParser eventParser) {
        // 读取一下历史订阅的filter信息
        List<ClientIdentity> clientIdentities = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentities) {
            subscribeChange(clientIdentity);
        }
    }

    /**
     * 停止binlog解析之前
     */
    @SuppressWarnings("unused")
    protected void beforeStopEventParser(CanalEventParser eventParser) {
    }

    /**
     * 停止binlog解析之后
     */
    protected void afterStopEventParser(CanalEventParser eventParser) {
        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {
                // 需要遍历启动
                stopEventParserInternal(singleEventParser);
            }
        } else {
            stopEventParserInternal(eventParser);
        }
    }

    /**
     * 初始化单个eventParser，不需要考虑group
     */
    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (!logPositionManager.isStart()) {
                logPositionManager.start();
            }
        }
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController instanceof HeartBeatHAController) {
                ((HeartBeatHAController) haController).setCanalHASwitchable(mysqlEventParser);
            }
            if (!haController.isStart()) {
                haController.start();
            }
        }
    }

    /**
     * 停止单个eventParser
     */
    protected void stopEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (logPositionManager.isStart()) {
                logPositionManager.stop();
            }
        }
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController.isStart()) {
                haController.stop();
            }
        }
    }
}
