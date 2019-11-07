package org.clever.canal.server.embedded;

import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.utils.CollectionUtils;
import org.clever.canal.common.utils.MigrateMap;
import org.clever.canal.instance.core.CanalInstance;
import org.clever.canal.instance.core.CanalInstanceGenerator;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.Message;
import org.clever.canal.protocol.SecurityUtil;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.protocol.position.Position;
import org.clever.canal.protocol.position.PositionRange;
import org.clever.canal.server.CanalServer;
import org.clever.canal.server.CanalService;
import org.clever.canal.server.exception.CanalServerException;
import org.clever.canal.spi.CanalMetricsProvider;
import org.clever.canal.spi.CanalMetricsService;
import org.clever.canal.spi.NopCanalMetricsService;
import org.clever.canal.store.CanalEventStore;
import org.clever.canal.store.memory.MemoryEventStoreWithBuffer;
import org.clever.canal.store.model.Event;
import org.clever.canal.store.model.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 嵌入式版本实现
 */
@SuppressWarnings("WeakerAccess")
public class CanalServerWithEmbedded extends AbstractCanalLifeCycle implements CanalServer, CanalService {
    private static final Logger logger = LoggerFactory.getLogger(CanalServerWithEmbedded.class);
    /**
     * 单例(一个JVM进程只能有一个实例)
     */
    public static final CanalServerWithEmbedded Instance = new CanalServerWithEmbedded();
    /**
     * Canal实例集合, destination --> CanalInstance
     */
    private Map<String, CanalInstance> canalInstances;
    /**
     * 根据 destination 创建 CanalInstance 的对象
     */
    @Getter
    @Setter
    private CanalInstanceGenerator canalInstanceGenerator;
    /**
     * 用户名
     */
    @Getter
    @Setter
    private String user;
    /**
     * 密码
     */
    @Getter
    @Setter
    private String password;
    /**
     * CanalServer 监控 metrics 实现
     */
    private CanalMetricsService metrics = NopCanalMetricsService.NOP;
    /**
     * CanalMetricsService 使用的端口
     */
    @Getter
    @Setter
    private int metricsPort = 18000;

    private CanalServerWithEmbedded() {
    }

    /**
     * 初始化 CanalServer
     */
    @Override
    public void start() {
        if (!isStart()) {
            super.start();
            // 如果存在provider,则启动 metrics service
            if (metrics != null) {
                loadCanalMetrics();
            }
            if (metrics != null) {
                metrics.setServerPort(metricsPort);
                metrics.initialize();
            }
            // 初始化Canal实例集合
            canalInstances = MigrateMap.makeComputingMap(canalInstanceGenerator::generate);
        }
    }

    /**
     * 停止 CanalServer
     */
    @Override
    public void stop() {
        super.stop();
        for (Map.Entry<String, CanalInstance> entry : canalInstances.entrySet()) {
            String destination = entry.getKey();
            CanalInstance canalInstance = entry.getValue();
            try {
                if (canalInstance.isStart()) {
                    MDC.put("destination", destination);
                    try {
                        canalInstance.stop();
                        logger.info("stop CanalInstances[{}] successfully", destination);
                    } finally {
                        MDC.remove("destination");
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("stop CanalInstance[%s] has an error", entry.getKey()), e);
            }
        }
        if (metrics.isRunning()) {
            metrics.terminate();
        }
    }

    /**
     * 客户端订阅，重复订阅时会更新对应的filter信息
     */
    @Override
    public void subscribe(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        if (!canalInstance.getMetaManager().isStart()) {
            canalInstance.getMetaManager().start();
        }
        // 执行一下meta订阅
        canalInstance.getMetaManager().subscribe(clientIdentity);
        Position position = canalInstance.getMetaManager().getCursor(clientIdentity);
        if (position == null) {
            // 获取一下store中的第一条
            position = canalInstance.getEventStore().getFirstPosition();
            if (position != null) {
                // 更新一下cursor
                canalInstance.getMetaManager().updateCursor(clientIdentity, position);
            }
            logger.info("subscribe successfully, {} with first position:{} ", clientIdentity, position);
        } else {
            logger.info("subscribe successfully, use last cursor position:{} {}", clientIdentity, position);
        }
        // 通知下订阅关系变化
        canalInstance.subscribeChange(clientIdentity);
    }

    /**
     * 取消订阅
     */
    @Override
    public void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException {
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        // 执行一下meta订阅
        canalInstance.getMetaManager().unsubscribe(clientIdentity);
        logger.info("unsubscribe successfully, {}", clientIdentity);
    }

    /**
     * 获取数据
     *
     * <pre>
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException {
        return get(clientIdentity, batchSize, null, null);
    }

    /**
     * 获取数据，可以指定超时时间.
     *
     * <pre>
     * 几种case:
     * a. 如果timeout为null，则采用tryGet方式，即时获取
     * b. 如果timeout不为null
     *    1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     *    2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @SuppressWarnings("DuplicatedCode")
    @Override
    public Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        // noinspection SynchronizationOnLocalVariableOrMethodParameter (压制警告)
        synchronized (canalInstance) {
            // 获取到流式数据中的最后一批获取的位置
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLatestBatch(clientIdentity);
            if (positionRanges != null) {
                throw new CanalServerException(String.format("clientId:%s has last batch:[%s] isn't ack , maybe loss data", clientIdentity.getClientId(), positionRanges));
            }
            Events<Event> events;
            Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
            events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);
            if (CollectionUtils.isEmpty(events.getEvents())) {
                logger.debug("get successfully, clientId:{} batchSize:{} but result is null", clientIdentity.getClientId(), batchSize);
                // 返回空包，避免生成batchId，浪费性能
                return new Message(-1, true);
            } else {
                // 记录到流式信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                List<CanalEntry.Entry> entries = Collections.emptyList();
                List<ByteString> rawEntries = Collections.emptyList();
                boolean raw = isRaw(canalInstance.getEventStore());
                if (raw) {
                    rawEntries = events.getEvents().stream().map(Event::getRawEntry).collect(Collectors.toList());
                } else {
                    entries = events.getEvents().stream().map(Event::getEntry).collect(Collectors.toList());
                }
                if (logger.isInfoEnabled()) {
                    logger.info(
                            "get successfully, clientId:{} batchSize:{} real size is {} and result is [batchId:{} , position:{}]",
                            clientIdentity.getClientId(),
                            batchSize,
                            raw ? rawEntries.size() : entries.size(),
                            batchId,
                            events.getPositionRange()
                    );
                }
                // 直接提交ack
                ack(clientIdentity, batchId);
                return new Message(batchId, entries, rawEntries);
            }
        }
    }

    /**
     * 不指定 position 获取事件。canal 会记住此 client 最新的 position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     *
     * <pre>
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException {
        return getWithoutAck(clientIdentity, batchSize, null, null);
    }

    /**
     * 不指定 position 获取事件。canal 会记住此 client 最新的 position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     *
     * <pre>
     * 几种case:
     * a. 如果timeout为null，则采用tryGet方式，即时获取
     * b. 如果timeout不为null
     *    1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     *    2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @SuppressWarnings("DuplicatedCode")
    @Override
    public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        // noinspection SynchronizationOnLocalVariableOrMethodParameter (压制警告)
        synchronized (canalInstance) {
            // 获取到流式数据中的最后一批获取的位置
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLatestBatch(clientIdentity);
            Events<Event> events;
            if (positionRanges != null) {
                // 存在流数据
                events = getEvents(canalInstance.getEventStore(), positionRanges.getStart(), batchSize, timeout, unit);
            } else {
                // ack后第一次获取
                Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
                if (start == null) {
                    // 第一次，还没有过ack记录，则获取当前store中的第一条
                    start = canalInstance.getEventStore().getFirstPosition();
                }
                events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);
            }
            if (CollectionUtils.isEmpty(events.getEvents())) {
                logger.debug("getWithoutAck successfully, clientId:{} batchSize:{} but result is null", clientIdentity.getClientId(), batchSize);
                // 返回空包，避免生成batchId，浪费性能
                return new Message(-1, true);
            } else {
                // 记录到流式信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                List<CanalEntry.Entry> entries = Collections.emptyList();
                List<ByteString> rawEntries = Collections.emptyList();
                boolean raw = isRaw(canalInstance.getEventStore());
                if (raw) {
                    rawEntries = events.getEvents().stream().map(Event::getRawEntry).collect(Collectors.toList());
                } else {
                    entries = events.getEvents().stream().map(Event::getEntry).collect(Collectors.toList());
                }
                if (logger.isInfoEnabled()) {
                    logger.info(
                            "getWithoutAck successfully, clientId:{} batchSize:{}  real size is {} and result is [batchId:{} , position:{}]",
                            clientIdentity.getClientId(),
                            batchSize,
                            raw ? rawEntries.size() : entries.size(),
                            batchId,
                            events.getPositionRange()
                    );
                }
                return new Message(batchId, entries, rawEntries);
            }
        }
    }

    /**
     * 进行 batch id 的确认。确认之后，小于等于此 batchId 的 Message 都会被确认。
     *
     * <pre>
     * 注意：进行反馈时必须按照batchId的顺序进行ack(需有客户端保证)
     * </pre>
     */
    @Override
    public void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        // 更新位置
        PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity, batchId);
        if (positionRanges == null) {
            // 说明是重复的ack/rollback
            throw new CanalServerException(String.format("ack error , clientId:%s batchId:%d is not exist , please check", clientIdentity.getClientId(), batchId));
        }
        // 更新cursor
        if (positionRanges.getAck() != null) {
            canalInstance.getMetaManager().updateCursor(clientIdentity, positionRanges.getAck());
            if (logger.isInfoEnabled()) {
                logger.info("ack successfully, clientId:{} batchId:{} position:{}", clientIdentity.getClientId(), batchId, positionRanges);
            }
        }
        // 可定时清理数据
        canalInstance.getEventStore().ack(positionRanges.getEnd(), positionRanges.getEndSeq());
    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    @Override
    public void rollback(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }
        // noinspection SynchronizationOnLocalVariableOrMethodParameter (压制警告)
        synchronized (canalInstance) {
            // 清除batch信息
            canalInstance.getMetaManager().clearAllBatches(clientIdentity);
            // rollback eventStore中的状态信息
            canalInstance.getEventStore().rollback();
            logger.info("rollback successfully, clientId:{}", new Object[]{clientIdentity.getClientId()});
        }
    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    @Override
    public void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }
        // noinspection SynchronizationOnLocalVariableOrMethodParameter (压制警告)
        synchronized (canalInstance) {
            // 清除batch信息
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity, batchId);
            // 说明是重复的ack/rollback
            if (positionRanges == null) {
                throw new CanalServerException(String.format("rollback error, clientId:%s batchId:%d is not exist , please check", clientIdentity.getClientId(), batchId));
            }
            // rollback TODO 后续rollback到指定的batchId位置
            canalInstance.getEventStore().rollback();
            // eventStore中的状态信息
            logger.info("rollback successfully, clientId:{} batchId:{} position:{}", clientIdentity.getClientId(), batchId, positionRanges);
        }
    }

    // ======================== public method =======================

    /**
     * 初始化 destination 对应的 CanalInstance
     */
    public void start(final String destination) {
        final CanalInstance canalInstance = canalInstances.get(destination);
        if (!canalInstance.isStart()) {
            try {
                MDC.put("destination", destination);
                if (metrics.isRunning()) {
                    metrics.register(canalInstance);
                }
                canalInstance.start();
                logger.info("start CanalInstances[{}] successfully", destination);
            } finally {
                MDC.remove("destination");
            }
        }
    }

    /**
     * 停止 destination 对应的 CanalInstance
     */
    @SuppressWarnings("unused")
    public void stop(final String destination) {
        CanalInstance canalInstance = canalInstances.remove(destination);
        if (canalInstance != null) {
            if (canalInstance.isStart()) {
                try {
                    MDC.put("destination", destination);
                    canalInstance.stop();
                    if (metrics.isRunning()) {
                        metrics.unregister(canalInstance);
                    }
                    logger.info("stop CanalInstances[{}] successfully", destination);
                } finally {
                    MDC.remove("destination");
                }
            }
        }
    }

    /**
     * CanalServer 授权处理
     *
     * @param user     用户名
     * @param password 密码
     * @param seed     认证数据签名(digest)
     */
    public boolean auth(String user, String password, byte[] seed) {
        // 如果user/password密码为空,则任何用户账户都能登录
        if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
            if (StringUtils.isEmpty(this.password)) {
                return true;
            } else if (StringUtils.isEmpty(password)) {
                // 如果server密码有配置,客户端密码为空,则拒绝
                return false;
            }
            try {
                byte[] passForClient = SecurityUtil.hexStr2Bytes(password);
                return SecurityUtil.scrambleServerAuth(passForClient, SecurityUtil.hexStr2Bytes(SecurityUtil.scrambleGenPass(this.password.getBytes())), seed);
            } catch (NoSuchAlgorithmException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * 查询所有的订阅信息
     */
    @SuppressWarnings("unused")
    public List<ClientIdentity> listAllSubscribe(String destination) throws CanalServerException {
        CanalInstance canalInstance = canalInstances.get(destination);
        return canalInstance.getMetaManager().listAllSubscribeInfo(destination);
    }

    /**
     * 查询当前未被ack的batch列表，batchId会按照从小到大进行返回
     */
    @SuppressWarnings("unused")
    public List<Long> listBatchIds(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        Map<Long, PositionRange> batches = canalInstance.getMetaManager().listAllBatches(clientIdentity);
        List<Long> result = new ArrayList<>(batches.keySet());
        Collections.sort(result);
        return result;
    }

    @SuppressWarnings("unused")
    public Map<String, CanalInstance> getCanalInstances() {
        return Collections.unmodifiableMap(canalInstances);
    }

    /**
     * 判断 destination 对应的 CanalInstance 是否正在运行
     */
    public boolean isStart(final String destination) {
        return canalInstances.containsKey(destination) && canalInstances.get(destination).isStart();
    }

    // ======================== helper method =======================

    /**
     * 通过Java的SPI机制加载 CanalMetricsService 的实现类
     */
    private void loadCanalMetrics() {
        ServiceLoader<CanalMetricsProvider> providers = ServiceLoader.load(CanalMetricsProvider.class);
        List<CanalMetricsProvider> list = new ArrayList<>();
        for (CanalMetricsProvider provider : providers) {
            list.add(provider);
        }
        if (!list.isEmpty()) {
            // 发现 provider, 进行初始化
            if (list.size() > 1) {
                logger.warn("Found more than one CanalMetricsProvider, use the first one.");
                // 报告冲突
                for (CanalMetricsProvider p : list) {
                    logger.warn("Found CanalMetricsProvider: {}.", p.getClass().getName());
                }
            }
            // 默认使用第一个
            CanalMetricsProvider provider = list.get(0);
            this.metrics = provider.getService();
        }
    }

    /**
     * 检查 destination 对应的 CanalInstance 正常运行，否则将抛出异常
     */
    private void checkStart(String destination) {
        if (!isStart(destination)) {
            throw new CanalServerException(String.format("destination:%s should start first", destination));
        }
    }

    /**
     * 检查clientIdentity是否已经订阅了，如果未定义则抛出异常
     */
    private void checkSubscribe(ClientIdentity clientIdentity) {
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            throw new CanalServerException(String.format("ClientIdentity:%s should subscribe first", clientIdentity.toString()));
        }
    }

    /**
     * 根据不同的参数，选择不同的方式获取数据
     */
    private Events<Event> getEvents(CanalEventStore<Event> eventStore, Position start, int batchSize, Long timeout, TimeUnit unit) {
        if (timeout == null) {
            return eventStore.tryGet(start, batchSize);
        } else {
            try {
                if (timeout <= 0) {
                    return eventStore.get(start, batchSize);
                } else {
                    return eventStore.get(start, batchSize, timeout, unit);
                }
            } catch (Exception e) {
                throw new CanalServerException(e);
            }
        }
    }

    /**
     * 判断 CanalEventStore 是否是使用的原始数据(raw)
     */
    private boolean isRaw(CanalEventStore eventStore) {
        if (eventStore instanceof MemoryEventStoreWithBuffer) {
            return ((MemoryEventStoreWithBuffer) eventStore).isRaw();
        }
        return true;
    }
}
