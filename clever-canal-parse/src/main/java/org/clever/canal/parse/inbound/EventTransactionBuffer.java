package org.clever.canal.parse.inbound;

import lombok.NoArgsConstructor;
import lombok.Setter;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.utils.Assert;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalEntry.EventType;
import org.clever.canal.store.exception.CanalStoreException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 缓冲event队列，提供按事务刷新数据的机制 (环形队列)
 */
@SuppressWarnings("WeakerAccess")
@NoArgsConstructor
public class EventTransactionBuffer extends AbstractCanalLifeCycle {
    /**
     * 队列位置
     */
    private static final long INIT_SEQUENCE = -1;
    /**
     * 队列位置(掩码)
     */
    private int indexMask;
    /**
     * 队列容量(size)
     */
    @Setter
    private int bufferSize = 1024;
    /**
     * 队列数组
     */
    private CanalEntry.Entry[] entries;
    /**
     * 代表当前put操作最后一次写操作发生的位置
     */
    private AtomicLong putSequence = new AtomicLong(INIT_SEQUENCE);
    /**
     * 代表满足flush条件后最后一次数据flush的时间
     */
    private AtomicLong flushSequence = new AtomicLong(INIT_SEQUENCE);
    /**
     * 事务刷新机制(事务刷新回调)
     */
    private TransactionFlushCallback flushCallback;

    public EventTransactionBuffer(TransactionFlushCallback flushCallback) {
        this.flushCallback = flushCallback;
    }

    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        Assert.notNull(flushCallback, "flush callback is null!");
        indexMask = bufferSize - 1;
        entries = new CanalEntry.Entry[bufferSize];
    }

    public void stop() throws CanalStoreException {
        putSequence.set(INIT_SEQUENCE);
        flushSequence.set(INIT_SEQUENCE);
        entries = null;
        super.stop();
    }

    public void add(List<CanalEntry.Entry> entryList) throws InterruptedException {
        for (CanalEntry.Entry entry : entryList) {
            add(entry);
        }
    }

    public void add(CanalEntry.Entry entry) throws InterruptedException {
        switch (entry.getEntryType()) {
            case TRANSACTION_BEGIN:
                // 刷新上一次的数据
                flush();
                put(entry);
                break;
            case TRANSACTION_END:
                // 事务已经结束
            case ENTRY_HEARTBEAT:
                // master过来的heartbeat，说明binlog已经读完了，是idle状态
                put(entry);
                flush();
                break;
            case ROW_DATA:
                put(entry);
                // 针对非DML的数据，直接输出，不进行buffer控制
                EventType eventType = entry.getHeader().getEventType();
                if (eventType != null && !isDml(eventType)) {
                    flush();
                }
                break;
            default:
                break;
        }
    }

    public void reset() {
        putSequence.set(INIT_SEQUENCE);
        flushSequence.set(INIT_SEQUENCE);
    }

    private void put(CanalEntry.Entry data) throws InterruptedException {
        // 首先检查是否有空位
        if (checkFreeSlotAt(putSequence.get() + 1)) {
            long current = putSequence.get();
            long next = current + 1;
            // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ringBuffer中的老的Entry值
            entries[getIndex(next)] = data;
            putSequence.set(next);
        } else {
            // buffer区满了，刷新一下
            flush();
            // 继续加一下新数据
            put(data);
        }
    }

    private void flush() throws InterruptedException {
        long start = this.flushSequence.get() + 1;
        long end = this.putSequence.get();
        if (start <= end) {
            List<CanalEntry.Entry> transaction = new ArrayList<>();
            for (long next = start; next <= end; next++) {
                transaction.add(this.entries[getIndex(next)]);
            }
            flushCallback.flush(transaction);
            // flush成功后，更新flush位置
            flushSequence.set(end);
        }
    }

    /**
     * 查询是否有空位
     */
    @SuppressWarnings("RedundantIfStatement")
    private boolean checkFreeSlotAt(final long sequence) {
        final long wrapPoint = sequence - bufferSize;
        if (wrapPoint > flushSequence.get()) {
            // 刚好追上一轮
            return false;
        } else {
            return true;
        }
    }

    private int getIndex(long sequence) {
        return (int) sequence & indexMask;
    }

    private boolean isDml(EventType eventType) {
        return eventType == EventType.INSERT || eventType == EventType.UPDATE || eventType == EventType.DELETE;
    }

    /**
     * 事务刷新机制
     */
    public interface TransactionFlushCallback {
        void flush(List<CanalEntry.Entry> transaction) throws InterruptedException;
    }
}
