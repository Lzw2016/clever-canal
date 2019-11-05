package org.clever.canal.instance.manager;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.CanalException;
import org.clever.canal.common.alarm.CanalAlarmHandler;
import org.clever.canal.common.alarm.LogAlarmHandler;
import org.clever.canal.common.utils.CollectionUtils;
import org.clever.canal.common.utils.JsonUtils;
import org.clever.canal.filter.aviater.AviaterRegexFilter;
import org.clever.canal.instance.core.AbstractCanalInstance;
import org.clever.canal.instance.manager.model.Canal;
import org.clever.canal.instance.manager.model.CanalParameter;
import org.clever.canal.instance.manager.model.CanalParameter.*;
import org.clever.canal.meta.FileMixedMetaManager;
import org.clever.canal.meta.MemoryMetaManager;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.ha.CanalHAController;
import org.clever.canal.parse.ha.HeartBeatHAController;
import org.clever.canal.parse.inbound.AbstractEventParser;
import org.clever.canal.parse.inbound.group.GroupEventParser;
import org.clever.canal.parse.inbound.mysql.LocalBinlogEventParser;
import org.clever.canal.parse.inbound.mysql.MysqlEventParser;
import org.clever.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy;
import org.clever.canal.parse.inbound.mysql.tsdb.TableMetaDataSourceConfig;
import org.clever.canal.parse.index.CanalLogPositionManager;
import org.clever.canal.parse.index.MetaLogPositionManager;
import org.clever.canal.parse.support.AuthenticationInfo;
import org.clever.canal.protocol.position.EntryPosition;
import org.clever.canal.sink.entry.EntryEventSink;
import org.clever.canal.sink.entry.group.GroupEventSink;
import org.clever.canal.store.AbstractCanalStoreScavenge;
import org.clever.canal.store.memory.MemoryEventStoreWithBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个canal实例，比如一个destination会独立一个实例
 */
@SuppressWarnings({"WeakerAccess"})
public class CanalInstanceWithManager extends AbstractCanalInstance {
    private static final Logger logger = LoggerFactory.getLogger(CanalInstanceWithManager.class);

    /**
     * 过滤表达式
     */
    protected String filter;
    /**
     * 对应参数
     */
    protected CanalParameter parameters;

    public CanalInstanceWithManager(Canal canal, String filter) {
        this.parameters = canal.getCanalParameter();
        this.canalId = canal.getId();
        this.destination = canal.getName();
        this.filter = filter;
        logger.info("[{}-{}] Init CanalInstance | parameters: {}", canalId, destination, parameters);
        // 初始化报警机制
        initAlarmHandler();
        // 初始化metaManager
        initMetaManager();
        // 初始化eventStore
        initEventStore();
        // 初始化eventSink
        initEventSink();
        // 初始化eventParser;
        initEventParser();
        // 基础工具，需要提前start，会有先订阅再根据filter条件启动parse的需求
        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }
        if (!metaManager.isStart()) {
            metaManager.start();
        }
        logger.info("[{}-{}] Init Successful!", canalId, destination);
    }

    @Override
    public void start() {
        // 初始化metaManager
        logger.info("[{}-{}] Starting CanalInstance | parameters: {}", canalId, destination, parameters);
        super.start();
    }

    @Override
    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            abstractEventParser.setAlarmHandler(getAlarmHandler());
        }
        super.startEventParserInternal(eventParser, isGroup);
    }

    /**
     * 初始化报警机制
     */
    protected void initAlarmHandler() {
        logger.info("[{}-{}] Init alarmHandler begin...", canalId, destination);
        AlarmMode mode = parameters.getAlarmMode();
        if (AlarmMode.LOGGER.equals(mode)) {
            alarmHandler = new LogAlarmHandler();
        } else {
            throw new CanalException("unsupported AlarmMode for " + mode);
        }
        logger.info("[{}-{}] Init alarmHandler end! -> load CanalAlarmHandler: {} ", canalId, destination, alarmHandler.getClass().getName());
    }

    /**
     * 初始化 metaManager
     */
    protected void initMetaManager() {
        logger.info("[{}-{}] Init metaManager begin...", canalId, destination);
        MetaMode mode = parameters.getMetaMode();
        if (MetaMode.MEMORY.equals(mode)) {
            metaManager = new MemoryMetaManager();
        } else if (MetaMode.LOCAL_FILE.equals(mode)) {
            metaManager = new FileMixedMetaManager(new File(parameters.getDataDir()), parameters.getMetaFileFlushPeriod());
        } else {
            throw new CanalException("unsupported MetaMode for " + mode);
        }
        logger.info("[{}-{}] Init metaManager end! -> load CanalMetaManager: {} ", canalId, destination, metaManager.getClass().getName());
    }

    /**
     * 初始化eventStore
     */
    protected void initEventStore() {
        logger.info("[{}-{}] Init eventStore begin...", canalId, destination);
        StorageMode mode = parameters.getStorageMode();
        if (StorageMode.MEMORY.equals(mode)) {
            MemoryEventStoreWithBuffer memoryEventStore = new MemoryEventStoreWithBuffer();
            memoryEventStore.setBufferSize(parameters.getMemoryStorageBufferSize());
            memoryEventStore.setBufferMemUnit(parameters.getMemoryStorageBufferMemUnit());
            memoryEventStore.setBatchMode(parameters.getStorageBatchMode());
            memoryEventStore.setRaw(parameters.getMemoryStorageRawEntry());
            memoryEventStore.setDdlIsolation(parameters.getDdlIsolation());
            eventStore = memoryEventStore;
        } else {
            throw new CanalException("unsupported StorageMode for " + mode);
        }
        // noinspection ConstantConditions (压制警告)
        if (eventStore instanceof AbstractCanalStoreScavenge) {
            StorageScavengeMode scavengeMode = parameters.getStorageScavengeMode();
            AbstractCanalStoreScavenge eventScavengeStore = (AbstractCanalStoreScavenge) eventStore;
            eventScavengeStore.setDestination(destination);
            eventScavengeStore.setCanalMetaManager(metaManager);
            eventScavengeStore.setOnAck(StorageScavengeMode.ON_ACK.equals(scavengeMode));
            eventScavengeStore.setOnFull(StorageScavengeMode.ON_FULL.equals(scavengeMode));
            eventScavengeStore.setOnSchedule(StorageScavengeMode.ON_SCHEDULE.equals(scavengeMode));
            if (StorageScavengeMode.ON_SCHEDULE.equals(scavengeMode)) {
                eventScavengeStore.setScavengeSchedule(parameters.getScavengeSchedule());
            }
        }
        logger.info("[{}-{}] Init eventStore end! -> load CanalEventStore: {}", canalId, destination, eventStore.getClass().getName());
    }

    /**
     * 初始化eventSink
     */
    protected void initEventSink() {
        logger.info("[{}-{}] Init eventSink begin...", canalId, destination);
        int groupSize = getGroupSize();
        if (groupSize <= 1) {
            eventSink = new EntryEventSink();
        } else {
            eventSink = new GroupEventSink(groupSize);
        }
        // noinspection ConstantConditions (压制警告)
        if (eventSink instanceof EntryEventSink) {
            ((EntryEventSink) eventSink).setFilterTransactionEntry(false);
            ((EntryEventSink) eventSink).setEventStore(getEventStore());
        }
        logger.info("[{}-{}] Init eventSink end! -> load CanalEventSink:{}", canalId, destination, eventSink.getClass().getName());
    }

    /**
     * 初始化eventParser
     */
    protected void initEventParser() {
        logger.info("[{}-{}] Init eventParser begin...", canalId, destination);
        SourcingType type = parameters.getSourcingType();
        List<List<DataSourcing>> groupDbAddresses = parameters.getGroupDbAddresses();
        if (!CollectionUtils.isEmpty(groupDbAddresses)) {
            // 取第一个分组的数量，主备分组的数量必须一致
            int size = groupDbAddresses.get(0).size();
            List<CanalEventParser> eventParsers = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<InetSocketAddress> dbAddress = new ArrayList<>();
                SourcingType lastType = null;
                for (List<DataSourcing> groupDbAddress : groupDbAddresses) {
                    if (lastType != null && !lastType.equals(groupDbAddress.get(i).getType())) {
                        throw new CanalException(String.format("master/slave Sourcing type is un match. %s vs %s", lastType, groupDbAddress.get(i).getType()));
                    }
                    lastType = groupDbAddress.get(i).getType();
                    dbAddress.add(groupDbAddress.get(i).getDbAddress());
                }
                if (lastType == null) {
                    throw new CanalException("SourcingType is not null");
                }
                // 初始化其中的一个分组parser
                eventParsers.add(doInitEventParser(lastType, dbAddress));
            }
            if (eventParsers.size() > 1) {
                // 如果存在分组，构造分组的parser
                GroupEventParser groupEventParser = new GroupEventParser();
                groupEventParser.setEventParsers(eventParsers);
                this.eventParser = groupEventParser;
            } else {
                this.eventParser = eventParsers.get(0);
            }
        } else {
            // 创建一个空数据库地址的parser，可能使用了tddl指定地址，启动的时候才会从tddl获取地址
            this.eventParser = doInitEventParser(type, new ArrayList<>());
        }
        logger.info("[{}-{}] Init eventParser end! -> load CanalEventParser:{}", canalId, destination, eventParser.getClass().getName());
    }

    /**
     * 初始化eventParser的实现
     */
    private CanalEventParser doInitEventParser(SourcingType type, List<InetSocketAddress> dbAddresses) {
        CanalEventParser eventParser;
        if (SourcingType.MYSQL.equals(type)) {
            MysqlEventParser mysqlEventParser;
            if (StringUtils.isNotEmpty(parameters.getRdsAccesskey())
                    && StringUtils.isNotEmpty(parameters.getRdsSecretKey())
                    && StringUtils.isNotEmpty(parameters.getRdsInstanceId())) {
                mysqlEventParser = new RdsBinlogEventParserProxy();
                ((RdsBinlogEventParserProxy) mysqlEventParser).setAccesskey(parameters.getRdsAccesskey());
                ((RdsBinlogEventParserProxy) mysqlEventParser).setSecretkey(parameters.getRdsSecretKey());
                ((RdsBinlogEventParserProxy) mysqlEventParser).setInstanceId(parameters.getRdsInstanceId());
            } else {
                mysqlEventParser = new MysqlEventParser();
            }
            mysqlEventParser.setDestination(destination);
            // 编码参数
            mysqlEventParser.setConnectionCharsetNumber(parameters.getConnectionCharsetNumber());
            mysqlEventParser.setConnectionCharset(Charset.forName(parameters.getConnectionCharset()));
            // 网络相关参数
            mysqlEventParser.setDefaultConnectionTimeoutInSeconds(parameters.getDefaultConnectionTimeoutInSeconds());
            mysqlEventParser.setSendBufferSize(parameters.getSendBufferSize());
            mysqlEventParser.setReceiveBufferSize(parameters.getReceiveBufferSize());
            // 心跳检查参数
            mysqlEventParser.setDetectingEnable(parameters.getDetectingEnable());
            mysqlEventParser.setDetectingSQL(parameters.getDetectingSQL());
            mysqlEventParser.setDetectingIntervalInSeconds(parameters.getDetectingIntervalInSeconds());
            // 数据库信息参数
            mysqlEventParser.setSlaveId(parameters.getSlaveId());
            if (!CollectionUtils.isEmpty(dbAddresses)) {
                mysqlEventParser.setMasterInfo(
                        new AuthenticationInfo(
                                dbAddresses.get(0),
                                parameters.getDbUsername(),
                                parameters.getDbPassword(),
                                parameters.getDefaultDatabaseName()
                        )
                );
                if (dbAddresses.size() > 1) {
                    mysqlEventParser.setStandbyInfo(
                            new AuthenticationInfo(
                                    dbAddresses.get(1),
                                    parameters.getDbUsername(),
                                    parameters.getDbPassword(),
                                    parameters.getDefaultDatabaseName()
                            )
                    );
                }
            }
            if (!CollectionUtils.isEmpty(parameters.getPositions())) {
                EntryPosition masterPosition = JsonUtils.unmarshalFromString(parameters.getPositions().get(0), EntryPosition.class);
                // binlog位置参数
                mysqlEventParser.setMasterPosition(masterPosition);
                if (parameters.getPositions().size() > 1) {
                    EntryPosition standbyPosition = JsonUtils.unmarshalFromString(parameters.getPositions().get(1), EntryPosition.class);
                    mysqlEventParser.setStandbyPosition(standbyPosition);
                }
            }
            mysqlEventParser.setFallbackIntervalInSeconds(parameters.getFallbackIntervalInSeconds());
            mysqlEventParser.setProfilingEnabled(false);
            mysqlEventParser.setFilterTableError(parameters.getFilterTableError());
            mysqlEventParser.setParallel(parameters.getParallel());
            mysqlEventParser.setGtIdMode(parameters.getGtIdEnable());
            // TsBb
            if (parameters.getTsdbSnapshotInterval() != null) {
                mysqlEventParser.setTsDbSnapshotInterval(parameters.getTsdbSnapshotInterval());
            }
            if (parameters.getTsdbSnapshotExpire() != null) {
                mysqlEventParser.setTsDbSnapshotExpire(parameters.getTsdbSnapshotExpire());
            }
//            TODO lzw
            boolean tsDbEnable = BooleanUtils.toBoolean(parameters.getTsdbEnable());
            if (tsDbEnable) {
                TableMetaDataSourceConfig dataSourceConfig = new TableMetaDataSourceConfig();
                dataSourceConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
                dataSourceConfig.setUrl(parameters.getTsdbJdbcUrl());
                dataSourceConfig.setUsername(parameters.getTsdbJdbcUserName());
                dataSourceConfig.setPassword(parameters.getTsdbJdbcPassword());
                mysqlEventParser.setDataSourceConfig(dataSourceConfig);
                mysqlEventParser.setEnableTsDb(tsDbEnable);
            }
            eventParser = mysqlEventParser;
        } else if (SourcingType.LOCAL_BINLOG.equals(type)) {
            LocalBinlogEventParser localBinlogEventParser = new LocalBinlogEventParser();
            localBinlogEventParser.setDestination(destination);
            localBinlogEventParser.setBufferSize(parameters.getReceiveBufferSize());
            localBinlogEventParser.setConnectionCharset(Charset.forName(parameters.getConnectionCharset()));
            localBinlogEventParser.setConnectionCharsetNumber(parameters.getConnectionCharsetNumber());
            localBinlogEventParser.setDirectory(parameters.getLocalBinlogDirectory());
            localBinlogEventParser.setProfilingEnabled(false);
            localBinlogEventParser.setDetectingEnable(parameters.getDetectingEnable());
            localBinlogEventParser.setDetectingIntervalInSeconds(parameters.getDetectingIntervalInSeconds());
            localBinlogEventParser.setFilterTableError(parameters.getFilterTableError());
            localBinlogEventParser.setParallel(parameters.getParallel());
            // 数据库信息，反查表结构时需要
            if (!CollectionUtils.isEmpty(dbAddresses)) {
                localBinlogEventParser.setMasterInfo(new AuthenticationInfo(
                                dbAddresses.get(0),
                                parameters.getDbUsername(),
                                parameters.getDbPassword(),
                                parameters.getDefaultDatabaseName()
                        )
                );
            }
            eventParser = localBinlogEventParser;
        } else if (SourcingType.ORACLE.equals(type)) {
            throw new CanalException("unsupported SourcingType for " + type);
        } else {
            throw new CanalException("unsupported SourcingType for " + type);
        }
        // add transaction support at 2012-12-06
        // noinspection ConstantConditions (压制警告)
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            abstractEventParser.setTransactionSize(parameters.getTransactionSize());
            abstractEventParser.setLogPositionManager(initLogPositionManager());
            abstractEventParser.setAlarmHandler(getAlarmHandler());
            abstractEventParser.setEventSink(getEventSink());
            if (StringUtils.isNotEmpty(filter)) {
                AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(filter);
                abstractEventParser.setEventFilter(aviaterFilter);
            }
            // 设置黑名单
            if (StringUtils.isNotEmpty(parameters.getBlackFilter())) {
                AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(parameters.getBlackFilter());
                abstractEventParser.setEventBlackFilter(aviaterFilter);
            }
        }
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            // 初始化haController，绑定与eventParser的关系，haController会控制eventParser
            CanalHAController haController = initHaController();
            mysqlEventParser.setHaController(haController);
        }
        return eventParser;
    }

    protected CanalHAController initHaController() {
        logger.info("init haController begin...");
        HAMode haMode = parameters.getHaMode();
        HeartBeatHAController haController;
        if (haMode.isHeartBeat()) {
            haController = new HeartBeatHAController();
            haController.setDetectingRetryTimes(parameters.getDetectingRetryTimes());
            haController.setSwitchEnable(parameters.getHeartbeatHaEnable());
        } else {
            throw new CanalException("unsupport HAMode for " + haMode);
        }
        logger.info("init haController end! \n\t load CanalHAController:{}", haController.getClass().getName());

        return haController;
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    protected CanalLogPositionManager initLogPositionManager() {
        logger.info("init logPositionPersistManager begin...");
        IndexMode indexMode = parameters.getIndexMode();
        CanalLogPositionManager logPositionManager = null;
        logPositionManager = new MetaLogPositionManager(metaManager);
//        logPositionManager = new FileMixedLogPositionManager(new File("./parse-manager"), 1000, new MemoryLogPositionManager());

//        TODO lzw
//        if (indexMode.isMemory()) {
//            logPositionManager = new MemoryLogPositionManager();
//        } else if (indexMode.isZookeeper()) {
//            logPositionManager = new ZooKeeperLogPositionManager(getZkclientx());
//        } else if (indexMode.isMixed()) {
//            MemoryLogPositionManager memoryLogPositionManager = new MemoryLogPositionManager();
//            ZooKeeperLogPositionManager zooKeeperLogPositionManager = new ZooKeeperLogPositionManager(getZkclientx());
//            logPositionManager = new PeriodMixedLogPositionManager(memoryLogPositionManager, zooKeeperLogPositionManager, 1000L);
//        } else if (indexMode.isMeta()) {
//            logPositionManager = new MetaLogPositionManager(metaManager);
//        } else if (indexMode.isMemoryMetaFailback()) {
//            MemoryLogPositionManager primary = new MemoryLogPositionManager();
//            MetaLogPositionManager secondary = new MetaLogPositionManager(metaManager);
//            logPositionManager = new FailbackLogPositionManager(primary, secondary);
//        } else {
//            throw new CanalException("unsupport indexMode for " + indexMode);
//        }
//        logger.info("init logPositionManager end! \n\t load CanalLogPositionManager:{}", logPositionManager.getClass().getName());
        return logPositionManager;
    }


    private int getGroupSize() {
        List<List<DataSourcing>> groupDbAddresses = parameters.getGroupDbAddresses();
        if (!CollectionUtils.isEmpty(groupDbAddresses)) {
            return groupDbAddresses.get(0).size();
        } else {
            // 可能是基于tddl的启动
            return 1;
        }
    }

//    private synchronized ZkClientx getZkclientx() {
//        // 做一下排序，保证相同的机器只使用同一个链接
//        List<String> zkClusters = new ArrayList<String>(parameters.getZkClusters());
//        Collections.sort(zkClusters);
//        return ZkClientx.getZkClient(StringUtils.join(zkClusters, ";"));
//    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }
}
