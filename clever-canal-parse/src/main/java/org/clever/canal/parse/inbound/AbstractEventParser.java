package org.clever.canal.parse.inbound;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.alarm.CanalAlarmHandler;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.dbsync.binlog.exception.TableIdNotFoundException;
import org.clever.canal.parse.driver.mysql.packets.GtIdSet;
import org.clever.canal.parse.driver.mysql.packets.MysqlGtIdSet;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.exception.PositionNotFoundException;
import org.clever.canal.parse.inbound.mysql.MysqlMultiStageCoprocessor;
import org.clever.canal.parse.index.CanalLogPositionManager;
import org.clever.canal.parse.support.AuthenticationInfo;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalEntry.Entry;
import org.clever.canal.protocol.CanalEntry.EntryType;
import org.clever.canal.protocol.CanalEntry.Header;
import org.clever.canal.protocol.position.EntryPosition;
import org.clever.canal.protocol.position.LogIdentity;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.sink.CanalEventSink;
import org.clever.canal.sink.exception.CanalSinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象的EventParser, 最大化共用mysql/oracle版本的实现
 */
@SuppressWarnings({"WeakerAccess"})
public abstract class AbstractEventParser<EVENT> extends AbstractCanalLifeCycle implements CanalEventParser<EVENT> {
    private final Logger logger = LoggerFactory.getLogger(AbstractEventParser.class);

    /**
     * 管理binlog消费位置信息
     */
    @Setter
    @Getter
    protected CanalLogPositionManager logPositionManager = null;
    /**
     * binlog event事件消费(连接parse 与 store)
     */
    @Setter
    protected CanalEventSink<List<CanalEntry.Entry>> eventSink = null;
    /**
     * 白名单过滤器
     */
    @Setter
    protected CanalEventFilter<String> eventFilter = null;
    /**
     * 黑名单过滤器
     */
    @Setter
    protected CanalEventFilter<String> eventBlackFilter = null;
    /**
     * canal报警处理
     */
    @Setter
    @Getter
    private CanalAlarmHandler alarmHandler = null;

    // ============================================================================================================================ 字段过滤
    /**
     * 字段过滤白名单字符串<br />
     * 分隔符：逗号(,)、冒号(:)、正斜杠(/) <br />
     * 规则：${tableName_1}:${field_1}/${field_2}/${field_3},${tableName_2}:${field_1}/${field_2}/${field_3},...
     */
    @Getter
    protected String fieldFilter;
    /**
     * 字段过滤白名单<br />
     * tableName --> List<fieldName>
     */
    @Getter
    protected Map<String, List<String>> fieldFilterMap = new HashMap<>();
    /**
     * 字段过滤黑名单字符串<br />
     * 分隔符：逗号(,)、冒号(:)、正斜杠(/) <br />
     * 规则：${tableName_1}:${field_1}/${field_2}/${field_3},${tableName_2}:${field_1}/${field_2}/${field_3},...
     */
    @Getter
    protected String fieldBlackFilter;
    /**
     * 字段过滤黑名单<br />
     * tableName --> List<fieldName>
     */
    @Getter
    protected Map<String, List<String>> fieldBlackFilterMap;

    // ============================================================================================================================ 统计参数
    /**
     * profile 开关参数
     */
    protected AtomicBoolean profilingEnabled = new AtomicBoolean(false);
    /**
     * 解析的binlog次数
     */
    protected AtomicLong parsedEventCount = new AtomicLong();
    /**
     * 消费的binlog次数
     */
    protected AtomicLong consumedEventCount = new AtomicLong();
    /**
     * 解析的时间间隔
     */
    @Getter
    protected long parsingInterval = -1;
    /**
     * 处理的时间间隔
     */
    @Getter
    protected long processingInterval = -1;

    // ============================================================================================================================ 数据库认证信息
    /**
     * 正在运行的数据库认证信息
     */
    protected volatile AuthenticationInfo runningInfo;
    /**
     * 通道名称
     */
    @Setter
    protected String destination;

    // ============================================================================================================================ 解析binlog
    /**
     * 解析binlog的真正实现
     */
    @Getter
    protected BinlogParser<EVENT> binlogParser = null;
    /**
     * 解析binlog的线程
     */
    protected Thread parseThread = null;
    /**
     * 解析binlog的线程异常处理
     */
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);
    /**
     * binlog数据环形缓冲队列
     */
    protected EventTransactionBuffer transactionBuffer;
    /**
     * binlog数据环形缓冲队列容量
     */
    @Setter
    protected int transactionSize = 1024;
    /**
     * 对事务的支持
     */
    protected AtomicBoolean needTransactionPosition = new AtomicBoolean(false);
    /**
     * 最后的binlog解析时间
     */
    protected long lastEntryTime = 0L;
    /**
     * 是否开启心跳检查
     */
    @Setter
    protected volatile boolean detectingEnable = true;
    /**
     * 心跳检查检测频率(单位秒)
     */
    @Setter
    protected Integer detectingIntervalInSeconds = 3;
    /**
     * 心跳任务触发时间控制
     */
    protected volatile Timer timer;
    /**
     * 心跳任务(Task)
     */
    protected TimerTask heartBeatTimerTask;
    /**
     * binlog解析产生的异常
     */
    @Getter
    protected Throwable exception = null;
    /**
     * 是否是GtId模式
     */
    @Setter
    @Getter
    protected boolean isGtIdMode = false;
    /**
     * 是否开启并行解析模式
     */
    @Setter
    @Getter
    protected boolean parallel = true;
    /**
     * 60%的CPU能力跑解析,剩余部分处理网络(解析的线程数量)
     */
    @Setter
    @Getter
    protected Integer parallelThreadSize = Runtime.getRuntime().availableProcessors() * 60 / 100;
    /**
     * 缓存区大小(必须为2的幂)
     */
    @Setter
    @Getter
    protected int parallelBufferSize = 256;
    /**
     * 针对解析器提供一个多阶段协同的处理
     */
    protected MultiStageCoprocessor multiStageCoprocessor;
    /**
     * 解析binlog的异常处理
     */
    @Setter
    @Getter
    protected ParserExceptionHandler parserExceptionHandler;
    /**
     * MySQL master 节点 serverId
     */
    @Setter
    @Getter
    protected long serverId;

    /**
     * 构建binlog解析器
     */
    protected abstract BinlogParser<EVENT> buildParser();

    /**
     * 构建数据库连接
     */
    protected abstract ErosaConnection buildErosaConnection();

    /**
     * 构建针对解析器提供一个多阶段协同的处理
     */
    protected abstract MultiStageCoprocessor buildMultiStageCoprocessor();

    /**
     * 查找解析binlog的开始位置
     *
     * @param connection 数据库连接
     */
    protected abstract EntryPosition findStartPosition(ErosaConnection connection) throws IOException;

    /**
     * 发送Dump请求之前的操作
     *
     * @param connection 数据库连接
     */
    protected void preDump(ErosaConnection connection) {
    }

    /**
     * 处理数据库表元数据(Meta)
     *
     * @param position binlog的位置
     */
    protected boolean processTableMeta(EntryPosition position) {
        return true;
    }

    /**
     * 发送Dump请求之后的操作
     *
     * @param connection 数据库连接
     */
    protected void afterDump(ErosaConnection connection) {
    }

    /**
     * 发动告警信息
     *
     * @param destination 通道名称
     * @param msg         告警消息
     */
    public void sendAlarm(String destination, String msg) {
        if (this.alarmHandler != null) {
            this.alarmHandler.sendAlarm(destination, msg);
        }
    }

    public AbstractEventParser() {
        // 初始化一下
        transactionBuffer = new EventTransactionBuffer(transaction -> {
            boolean successful = consumeTheEventAndProfilingIfNecessary(transaction);
            if (!running) {
                return;
            }
            if (!successful) {
                throw new CanalParseException("consume failed!");
            }
            LogPosition position = buildLastTransactionPosition(transaction);
            if (position != null) {
                // 可能position为空
                logPositionManager.persistLogPosition(AbstractEventParser.this.destination, position);
            }
        });
    }

    /**
     * 开始解析binlog
     */
    public void start() {
        super.start();
        MDC.put("destination", destination);
        // 设置buffer大小
        transactionBuffer.setBufferSize(transactionSize);
        // 初始化缓冲队列
        transactionBuffer.start();

        // 构造bin log parser
        binlogParser = buildParser();
        // 初始化一下BinLogParser
        binlogParser.start();

        // 启动工作线程
        parseThread = new Thread(new Runnable() {
            public void run() {
                MDC.put("destination", destination);
                ErosaConnection erosaConnection = null;
                while (running) {
                    try {
                        // 开始执行replication
                        // 1. 构造Erosa连接
                        erosaConnection = buildErosaConnection();
                        // 2. 启动一个心跳线程
                        startHeartBeat(erosaConnection);
                        // 3. 执行dump前的准备工作
                        preDump(erosaConnection);
                        // 连接数据库服务器
                        erosaConnection.connect();
                        long queryServerId = erosaConnection.queryServerId();
                        if (queryServerId != 0) {
                            serverId = queryServerId;
                        }
                        // 4. 获取最后的位置信息
                        long start = System.currentTimeMillis();
                        logger.info("---> begin to find start position, it will be long time for reset or first position");
                        final EntryPosition startPosition = findStartPosition(erosaConnection);
                        if (startPosition == null) {
                            throw new PositionNotFoundException("can't find start position for " + destination);
                        }
                        if (!processTableMeta(startPosition)) {
                            throw new CanalParseException("can't find init table meta for " + destination + " with position : " + startPosition);
                        }
                        long end = System.currentTimeMillis();
                        logger.info("---> find start position successfully, {}", startPosition.toString() + " cost : " + (end - start) + "ms , the next step is binlog dump");
                        // 重新链接，因为在找position过程中可能有状态，需要断开后重建
                        erosaConnection.reconnect();
                        final SinkFunction sinkHandler = new SinkFunction<EVENT>() {
                            private LogPosition lastPosition;

                            public boolean sink(EVENT event) {
                                try {
                                    CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, false);
                                    if (!running) {
                                        return false;
                                    }
                                    if (entry != null) {
                                        // 有正常数据流过，清空exception
                                        exception = null;
                                        transactionBuffer.add(entry);
                                        // 记录一下对应的positions
                                        this.lastPosition = buildLastPosition(entry);
                                        // 记录一下最后一次有数据的时间
                                        lastEntryTime = System.currentTimeMillis();
                                    }
                                    return running;
                                } catch (TableIdNotFoundException e) {
                                    throw e;
                                } catch (Throwable e) {
                                    if (e.getCause() instanceof TableIdNotFoundException) {
                                        throw (TableIdNotFoundException) e.getCause();
                                    }
                                    // 记录一下，出错的位点信息
                                    processSinkError(e, this.lastPosition, startPosition.getJournalName(), startPosition.getPosition());
                                    // 继续抛出异常，让上层统一感知
                                    throw new CanalParseException(e);
                                }
                            }
                        };
                        // 4. 开始dump数据
                        if (parallel) {
                            // build stage processor
                            multiStageCoprocessor = buildMultiStageCoprocessor();
                            if (isGtIdMode() && StringUtils.isNotEmpty(startPosition.getGtId())) {
                                // 判断所属instance是否启用GtId模式，是的话调用ErosaConnection中GtId对应方法dump数据
                                GtIdSet gtIdSet = MysqlGtIdSet.parse(startPosition.getGtId());
                                ((MysqlMultiStageCoprocessor) multiStageCoprocessor).setGtidSet(gtIdSet);
                                multiStageCoprocessor.start();
                                erosaConnection.dump(gtIdSet, multiStageCoprocessor);
                            } else {
                                multiStageCoprocessor.start();
                                if (StringUtils.isEmpty(startPosition.getJournalName()) && startPosition.getTimestamp() != null) {
                                    erosaConnection.dump(startPosition.getTimestamp(), multiStageCoprocessor);
                                } else {
                                    erosaConnection.dump(startPosition.getJournalName(), startPosition.getPosition(), multiStageCoprocessor);
                                }
                            }
                        } else {
                            if (isGtIdMode() && StringUtils.isNotEmpty(startPosition.getGtId())) {
                                // 判断所属instance是否启用GtId模式，是的话调用ErosaConnection中GtId对应方法dump数据
                                erosaConnection.dump(MysqlGtIdSet.parse(startPosition.getGtId()), sinkHandler);
                            } else {
                                if (StringUtils.isEmpty(startPosition.getJournalName()) && startPosition.getTimestamp() != null) {
                                    erosaConnection.dump(startPosition.getTimestamp(), sinkHandler);
                                } else {
                                    erosaConnection.dump(startPosition.getJournalName(), startPosition.getPosition(), sinkHandler);
                                }
                            }
                        }
                    } catch (TableIdNotFoundException e) {
                        exception = e;
                        // 特殊处理TableIdNotFound异常,出现这样的异常，一种可能就是起始的position是一个事务当中，导致table map
                        // Event时间没解析过
                        needTransactionPosition.compareAndSet(false, true);
                        logger.error(String.format("dump address %s has an error, retrying. caused by ", runningInfo.getAddress().toString()), e);
                    } catch (Throwable e) {
                        processDumpError(e);
                        exception = e;
                        if (!running) {
                            if (!(e instanceof java.nio.channels.ClosedByInterruptException || e.getCause() instanceof java.nio.channels.ClosedByInterruptException)) {
                                throw new CanalParseException(String.format("dump address %s has an error, retrying. ", runningInfo.getAddress().toString()), e);
                            }
                        } else {
                            logger.error(String.format("dump address %s has an error, retrying. caused by ", runningInfo.getAddress().toString()), e);
                            sendAlarm(destination, ExceptionUtils.getStackTrace(e));
                        }
                        if (parserExceptionHandler != null) {
                            parserExceptionHandler.handle(e);
                        }
                    } finally {
                        // 重新置为中断状态
                        // noinspection ResultOfMethodCallIgnored (压制警告)
                        Thread.interrupted();
                        // 关闭一下链接
                        afterDump(erosaConnection);
                        try {
                            if (erosaConnection != null) {
                                erosaConnection.disconnect();
                            }
                        } catch (IOException e1) {
                            if (!running) {
                                // noinspection ThrowFromFinallyBlock (压制警告)
                                throw new CanalParseException(String.format("disconnect address %s has an error, retrying. ", runningInfo.getAddress().toString()), e1);
                            } else {
                                logger.error("disconnect address {} has an error, retrying., caused by ", runningInfo.getAddress().toString(), e1);
                            }
                        }
                    }
                    // 出异常了，退出sink消费，释放一下状态
                    eventSink.interrupt();
                    // 重置一下缓冲队列，重新记录数据
                    transactionBuffer.reset();
                    // 重新置位
                    binlogParser.reset();
                    if (multiStageCoprocessor != null && multiStageCoprocessor.isStart()) {
                        // 处理 RejectedExecutionException
                        try {
                            multiStageCoprocessor.stop();
                        } catch (Throwable t) {
                            logger.debug("multi processor rejected:", t);
                        }
                    }
                    if (running) {
                        // sleep一段时间再进行重试(10 ~ 20 秒)
                        try {
                            Thread.sleep(10000 + RandomUtils.nextInt(0, 10000));
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
                MDC.remove("destination");
            }
        });
        parseThread.setUncaughtExceptionHandler(handler);
        parseThread.setName(String.format("destination = %s , address = %s , EventParser", destination, runningInfo == null ? null : runningInfo.getAddress()));
        parseThread.start();
    }

    /**
     * 停止解析binlog
     */
    public void stop() {
        super.stop();
        // 先停止心跳
        stopHeartBeat();
        // 尝试中断
        parseThread.interrupt();
        eventSink.interrupt();
        if (multiStageCoprocessor != null && multiStageCoprocessor.isStart()) {
            try {
                multiStageCoprocessor.stop();
            } catch (Throwable t) {
                logger.debug("multi processor rejected:", t);
            }
        }
        try {
            // 等待其结束
            parseThread.join();
        } catch (InterruptedException ignored) {
        }
        if (binlogParser.isStart()) {
            binlogParser.stop();
        }
        if (transactionBuffer.isStart()) {
            transactionBuffer.stop();
        }
    }

    protected boolean consumeTheEventAndProfilingIfNecessary(List<CanalEntry.Entry> entries) throws CanalSinkException, InterruptedException {
        long startTs = -1;
        boolean enabled = getProfilingEnabled();
        if (enabled) {
            startTs = System.currentTimeMillis();
        }
        boolean result = eventSink.sink(entries, (runningInfo == null) ? null : runningInfo.getAddress(), destination);
        if (enabled) {
            this.processingInterval = System.currentTimeMillis() - startTs;
        }
        if (consumedEventCount.incrementAndGet() < 0) {
            consumedEventCount.set(0);
        }
        return result;
    }

    protected CanalEntry.Entry parseAndProfilingIfNecessary(EVENT bod, boolean isSeek) {
        long startTs = -1;
        boolean enabled = getProfilingEnabled();
        if (enabled) {
            startTs = System.currentTimeMillis();
        }
        CanalEntry.Entry event = binlogParser.parse(bod, isSeek);
        if (enabled) {
            this.parsingInterval = System.currentTimeMillis() - startTs;
        }
        if (parsedEventCount.incrementAndGet() < 0) {
            parsedEventCount.set(0);
        }
        return event;
    }

    public Boolean getProfilingEnabled() {
        return profilingEnabled.get();
    }

    // 初始化一下
    protected LogPosition buildLastTransactionPosition(List<CanalEntry.Entry> entries) {
        for (int i = entries.size() - 1; i > 0; i--) {
            CanalEntry.Entry entry = entries.get(i);
            // 尽量记录一个事务做为position
            if (entry.getEntryType() == EntryType.TRANSACTION_END) {
                return buildLastPosition(entry);
            }
        }
        return null;
    }

    // 初始化一下
    protected LogPosition buildLastPosition(CanalEntry.Entry entry) {
        LogPosition logPosition = new LogPosition();
        EntryPosition position = new EntryPosition();
        position.setJournalName(entry.getHeader().getLogfileName());
        position.setPosition(entry.getHeader().getLogfileOffset());
        position.setTimestamp(entry.getHeader().getExecuteTime());
        // add serverId at 2016-06-28
        position.setServerId(entry.getHeader().getServerId());
        // set GtId
        position.setGtId(entry.getHeader().getGtId());
        logPosition.setPosition(position);
        LogIdentity identity = new LogIdentity(runningInfo.getAddress(), -1L);
        logPosition.setIdentity(identity);
        return logPosition;
    }

    protected void processSinkError(Throwable e, LogPosition lastPosition, String startBinlogFile, Long startPosition) {
        if (lastPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s]", lastPosition.getPosition()), e);
        } else {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s,%s]", startBinlogFile, startPosition), e);
        }
    }

    /**
     * 处理Dump异常
     */
    protected void processDumpError(Throwable e) {
        // do nothing
    }

    protected void startHeartBeat(ErosaConnection connection) {
        // 初始化
        lastEntryTime = 0L;
        // lazy初始化一下
        if (timer == null) {
            String name = String.format("destination = %s , address = %s , HeartBeatTimeTask", destination, runningInfo == null ? null : runningInfo.getAddress().toString());
            synchronized (AbstractEventParser.class) {
                // synchronized (MysqlEventParser.class) {
                // why use MysqlEventParser.class, u know, MysqlEventParser is the child class 4 AbstractEventParser, do this is ...
                if (timer == null) {
                    timer = new Timer(name, true);
                }
            }
        }
        // fixed issue #56，避免重复创建heartbeat线程
        if (heartBeatTimerTask == null) {
            heartBeatTimerTask = buildHeartBeatTimeTask(connection);
            Integer interval = detectingIntervalInSeconds;
            timer.schedule(heartBeatTimerTask, interval * 1000L, interval * 1000L);
            logger.info("start heart beat.... ");
        }
    }

    /**
     * 构建心跳任务
     */
    protected TimerTask buildHeartBeatTimeTask(ErosaConnection connection) {
        return new TimerTask() {
            public void run() {
                try {
                    if (exception == null || lastEntryTime > 0) {
                        // 如果未出现异常，或者有第一条正常数据
                        long now = System.currentTimeMillis();
                        long interval = (now - lastEntryTime) / 1000;
                        if (interval >= detectingIntervalInSeconds) {
                            Header.Builder headerBuilder = Header.newBuilder();
                            headerBuilder.setExecuteTime(now);
                            Entry.Builder entryBuilder = Entry.newBuilder();
                            entryBuilder.setHeader(headerBuilder.build());
                            entryBuilder.setEntryType(EntryType.ENTRY_HEARTBEAT);
                            Entry entry = entryBuilder.build();
                            // 提交到sink中，目前不会提交到store中，会在sink中进行忽略
                            consumeTheEventAndProfilingIfNecessary(Collections.singletonList(entry));
                        }
                    }
                } catch (Throwable e) {
                    logger.warn("heartBeat run failed ", e);
                }
            }
        };
    }

    /**
     * 停止心跳任务
     */
    protected void stopHeartBeat() {
        // 初始化
        lastEntryTime = 0L;
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        heartBeatTimerTask = null;
    }

    /**
     * 解析字段过滤规则
     */
    private Map<String, List<String>> parseFieldFilterMap(String config) {
        Map<String, List<String>> map = new HashMap<>();
        if (StringUtils.isNotBlank(config)) {
            for (String filter : config.split(",")) {
                if (StringUtils.isBlank(filter)) {
                    continue;
                }
                String[] filterConfig = filter.split(":");
                if (filterConfig.length != 2) {
                    continue;
                }
                map.put(filterConfig[0].trim().toUpperCase(), Arrays.asList(filterConfig[1].trim().toUpperCase().split("/")));
            }
        }
        return map;
    }

    @SuppressWarnings("unused")
    public Long getParsedEventCount() {
        return parsedEventCount.get();
    }

    @SuppressWarnings("unused")
    public Long getConsumedEventCount() {
        return consumedEventCount.get();
    }

    public void setProfilingEnabled(boolean profilingEnabled) {
        this.profilingEnabled = new AtomicBoolean(profilingEnabled);
    }

    public void setFieldFilter(String fieldFilter) {
        this.fieldFilter = fieldFilter.trim();
        this.fieldFilterMap = parseFieldFilterMap(fieldFilter);
    }

    public void setFieldBlackFilter(String fieldBlackFilter) {
        this.fieldBlackFilter = fieldBlackFilter;
        this.fieldBlackFilterMap = parseFieldFilterMap(fieldBlackFilter);
    }
}
