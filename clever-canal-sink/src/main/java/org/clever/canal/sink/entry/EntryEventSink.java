package org.clever.canal.sink.entry;

import org.clever.canal.common.utils.Assert;
import org.clever.canal.common.utils.CollectionUtils;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalEntry.Entry;
import org.clever.canal.protocol.CanalEntry.EntryType;
import org.clever.canal.protocol.position.LogIdentity;
import org.clever.canal.sink.AbstractCanalEventSink;
import org.clever.canal.sink.CanalEventDownStreamHandler;
import org.clever.canal.sink.CanalEventSink;
import org.clever.canal.sink.exception.CanalSinkException;
import org.clever.canal.store.CanalEventStore;
import org.clever.canal.store.memory.MemoryEventStoreWithBuffer;
import org.clever.canal.store.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * mysql binlog数据对象输出
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class EntryEventSink extends AbstractCanalEventSink<List<CanalEntry.Entry>> implements CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger logger = LoggerFactory.getLogger(EntryEventSink.class);
    private static final int maxFullTimes = 10;
    private CanalEventStore<Event> eventStore;
    /**
     * 是否需要尽可能过滤事务头/尾
     */
    protected boolean filterTransactionEntry = false;
    /**
     * 是否需要过滤空的事务头/尾
     */
    protected boolean filterEmptyTransactionEntry = true;
    /**
     * 空的事务输出的频率
     */
    protected long emptyTransactionInterval = 5 * 1000;
    /**
     * 超过8192个事务头，输出一个
     */
    protected long emptyTransactionThreshold = 8192;

    protected volatile long lastTransactionTimestamp = 0L;
    protected AtomicLong lastTransactionCount = new AtomicLong(0L);
    protected volatile long lastEmptyTransactionTimestamp = 0L;
    protected AtomicLong lastEmptyTransactionCount = new AtomicLong(0L);
    protected AtomicLong eventsSinkBlockingTime = new AtomicLong(0L);
    protected boolean raw;

    public EntryEventSink() {
        addHandler(new HeartBeatEntryEventHandler());
    }

    public void start() {
        super.start();
        Assert.notNull(eventStore);
        if (eventStore instanceof MemoryEventStoreWithBuffer) {
            this.raw = ((MemoryEventStoreWithBuffer) eventStore).isRaw();
        }
        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (!handler.isStart()) {
                handler.start();
            }
        }
    }

    public void stop() {
        super.stop();
        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (handler.isStart()) {
                handler.stop();
            }
        }
    }

    public boolean filter(List<Entry> event, InetSocketAddress remoteAddress, String destination) {
        return false;
    }

    public boolean sink(List<CanalEntry.Entry> entryList, InetSocketAddress remoteAddress, String destination) throws CanalSinkException {
        return sinkData(entryList, remoteAddress);
    }

    private boolean sinkData(List<CanalEntry.Entry> entryList, InetSocketAddress remoteAddress) {
        boolean hasRowData = false;
        boolean hasHeartBeat = false;
        List<Event> events = new ArrayList<>();
        for (CanalEntry.Entry entry : entryList) {
            if (!doFilter(entry)) {
                continue;
            }
            if (filterTransactionEntry && (entry.getEntryType() == EntryType.TRANSACTION_BEGIN || entry.getEntryType() == EntryType.TRANSACTION_END)) {
                long currentTimestamp = entry.getHeader().getExecuteTime();
                // 基于一定的策略控制，放过空的事务头和尾，便于及时更新数据库位点，表明工作正常
                if (lastTransactionCount.incrementAndGet() <= emptyTransactionThreshold && Math.abs(currentTimestamp - lastTransactionTimestamp) <= emptyTransactionInterval) {
                    continue;
                } else {
                    lastTransactionCount.set(0L);
                    lastTransactionTimestamp = currentTimestamp;
                }
            }
            hasRowData |= (entry.getEntryType() == EntryType.ROW_DATA);
            hasHeartBeat |= (entry.getEntryType() == EntryType.ENTRY_HEARTBEAT);
            Event event = new Event(new LogIdentity(remoteAddress, -1L), entry, raw);
            events.add(event);
        }
        if (hasRowData || hasHeartBeat) {
            // 存在row记录 或者 存在heartbeat记录，直接跳给后续处理
            return doSink(events);
        } else {
            // 需要过滤的数据
            if (filterEmptyTransactionEntry && !CollectionUtils.isEmpty(events)) {
                long currentTimestamp = events.get(0).getExecuteTime();
                // 基于一定的策略控制，放过空的事务头和尾，便于及时更新数据库位点，表明工作正常
                if (Math.abs(currentTimestamp - lastEmptyTransactionTimestamp) > emptyTransactionInterval || lastEmptyTransactionCount.incrementAndGet() > emptyTransactionThreshold) {
                    lastEmptyTransactionCount.set(0L);
                    lastEmptyTransactionTimestamp = currentTimestamp;
                    return doSink(events);
                }
            }
            // 直接返回true，忽略空的事务头和尾
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    protected boolean doFilter(CanalEntry.Entry entry) {
        if (filter != null && entry.getEntryType() == EntryType.ROW_DATA) {
            String name = getSchemaNameAndTableName(entry);
            boolean need = filter.filter(name);
            if (!need) {
                logger.debug("filter name[{}] entry : {}:{}", name, entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset());
            }
            return need;
        } else {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    protected boolean doSink(List<Event> events) {
        for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
            events = handler.before(events);
        }
        long blockingStart = 0L;
        int fullTimes = 0;
        do {
            if (eventStore.tryPut(events)) {
                if (fullTimes > 0) {
                    eventsSinkBlockingTime.addAndGet(System.nanoTime() - blockingStart);
                }
                for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                    events = handler.after(events);
                }
                return true;
            } else {
                if (fullTimes == 0) {
                    blockingStart = System.nanoTime();
                }
                applyWait(++fullTimes);
                if (fullTimes % 100 == 0) {
                    long nextStart = System.nanoTime();
                    eventsSinkBlockingTime.addAndGet(nextStart - blockingStart);
                    blockingStart = nextStart;
                }
            }
            for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                events = handler.retry(events);
            }
        } while (running && !Thread.interrupted());
        return false;
    }

    // 处理无数据的情况，避免空循环挂死
    private void applyWait(int fullTimes) {
        int newFullTimes = Math.min(fullTimes, maxFullTimes);
        // 3次以内
        if (fullTimes <= 3) {
            Thread.yield();
        } else {
            // 超过3次，最多只sleep 10ms
            LockSupport.parkNanos(1000 * 1000L * newFullTimes);
        }
    }

    private String getSchemaNameAndTableName(CanalEntry.Entry entry) {
        return entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public void setFilterEmptyTransactionEntry(boolean filterEmptyTransactionEntry) {
        this.filterEmptyTransactionEntry = filterEmptyTransactionEntry;
    }

    public void setEmptyTransactionInterval(long emptyTransactionInterval) {
        this.emptyTransactionInterval = emptyTransactionInterval;
    }

    public void setEmptyTransactionThreshold(long emptyTransactionThreshold) {
        this.emptyTransactionThreshold = emptyTransactionThreshold;
    }

    public AtomicLong getEventsSinkBlockingTime() {
        return eventsSinkBlockingTime;
    }
}
