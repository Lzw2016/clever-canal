package org.clever.canal.store.memory;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalEntry.EventType;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.protocol.position.Position;
import org.clever.canal.protocol.position.PositionRange;
import org.clever.canal.store.AbstractCanalStoreScavenge;
import org.clever.canal.store.CanalEventStore;
import org.clever.canal.store.CanalStoreScavenge;
import org.clever.canal.store.exception.CanalStoreException;
import org.clever.canal.store.helper.CanalEventUtils;
import org.clever.canal.store.model.BatchMode;
import org.clever.canal.store.model.Event;
import org.clever.canal.store.model.Events;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于内存buffer构建内存memory store
 *
 * <pre>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 *   a. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 * </pre>
 */
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge implements CanalEventStore<Event>, CanalStoreScavenge {
    /**
     * 初始状态下环形队列的位置
     */
    private static final long INIT_SEQUENCE = -1;
    /**
     * 环形队列的大小
     */
    @Getter
    @Setter
    private int bufferSize = 16 * 1024;
    /**
     * memSize的单位，默认为1kb大小
     */
    @Getter
    @Setter
    private int bufferMemUnit = 1024;
    /**
     * 用于对putSequence、getSequence、ackSequence进行取余操作，canal通过位操作进行取余，其值为 bufferSize-1
     */
    private int indexMask;
    /**
     * 环形队列数据数组
     */
    private Event[] entries;

    // ================================================================================================= 记录下put/get/ack操作的三个下标

    /**
     * 代表当前put操作最后一次写操作发生的位置
     */
    private AtomicLong putSequence = new AtomicLong(INIT_SEQUENCE);
    /**
     * 代表当前get操作读取的最后一条的位置
     */
    private AtomicLong getSequence = new AtomicLong(INIT_SEQUENCE);
    /**
     * 代表当前ack操作的最后一条的位置
     */
    private AtomicLong ackSequence = new AtomicLong(INIT_SEQUENCE);

    // ================================================================================================= 记录下put/get/ack操作的三个memSize大小

    /**
     * put操作的memSize大小
     */
    private AtomicLong putMemSize = new AtomicLong(0);
    /**
     * get操作的memSize大小
     */
    private AtomicLong getMemSize = new AtomicLong(0);
    /**
     * ack操作的memSize大小
     */
    private AtomicLong ackMemSize = new AtomicLong(0);

    // ================================================================================================= 记录下put/get/ack操作的三个execTime

    /**
     * put操作的execTime
     */
    private AtomicLong putExecTime = new AtomicLong(System.currentTimeMillis());
    /**
     * get操作的execTime
     */
    private AtomicLong getExecTime = new AtomicLong(System.currentTimeMillis());
    /**
     * ack操作的execTime
     */
    private AtomicLong ackExecTime = new AtomicLong(System.currentTimeMillis());

    // ================================================================================================= 记录下put/get/ack操作的三个table rows

    /**
     * put操作的table rows
     */
    private AtomicLong putTableRows = new AtomicLong(0);
    /**
     * get操作的table rows
     */
    private AtomicLong getTableRows = new AtomicLong(0);
    /**
     * ack操作的table rows
     */
    private AtomicLong ackTableRows = new AtomicLong(0);

    // ================================================================================================= 阻塞put/get操作控制信号

    /**
     * put操作和get操作共用一把锁(lock)
     */
    private ReentrantLock lock = new ReentrantLock();
    /**
     * notFull用于控制put操作，只有队列没满的情况下才能put
     */
    private Condition notFull = lock.newCondition();
    /**
     * notEmpty控制get操作，只有队列不为空的情况下，才能get
     */
    private Condition notEmpty = lock.newCondition();

    // ================================================================================================= MemoryEventStoreWithBuffer配置

    /**
     * 默认为对象数量模式
     */
    @Getter
    @Setter
    private BatchMode batchMode = BatchMode.ITEM_SIZE;
    /**
     * 是否需要DDL隔离
     */
    @Getter
    @Setter
    private boolean ddlIsolation = false;
    /**
     * 针对entry是否开启raw模式<br />
     * 使用server/client模式时, 建议开启raw模式(raw = true)<br />
     * 使用Embedded模式时, 建议关闭raw模式(raw = false)<br />
     */
    @Getter
    @Setter
    private boolean raw = true;

    public MemoryEventStoreWithBuffer() {
    }

    @Override
    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        indexMask = bufferSize - 1;
        entries = new Event[bufferSize];
    }

    @Override
    public void stop() throws CanalStoreException {
        super.stop();
        cleanAll();
    }

    @Override
    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                // 检查是否有空位
                while (!checkFreeSlotAt(putSequence.get() + data.size())) {
                    // wait until not full
                    notFull.await();
                }
            } catch (InterruptedException ie) {
                // propagate to non-interrupted thread
                notFull.signal();
                throw ie;
            }
            doPut(data);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                if (checkFreeSlotAt(putSequence.get() + data.size())) {
                    doPut(data);
                    return true;
                }
                if (nanos <= 0) {
                    return false;
                }

                try {
                    nanos = notFull.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    // propagate to non-interrupted thread
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!checkFreeSlotAt(putSequence.get() + data.size())) {
                return false;
            } else {
                doPut(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Collections.singletonList(data));
    }

    @Override
    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Collections.singletonList(data), timeout, unit);
    }

    @Override
    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Collections.singletonList(data));
    }

    /**
     * 执行具体的put操作
     */
    private void doPut(List<Event> data) {
        long current = putSequence.get();
        long end = current + data.size();

        // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ringbuffer中的老的Entry值
        for (long next = current + 1; next <= end; next++) {
            entries[getIndex(next)] = data.get((int) (next - current - 1));
        }

        putSequence.set(end);

        // 记录一下gets memsize信息，方便快速检索
        if (batchMode.isMemSize()) {
            long size = 0;
            for (Event event : data) {
                size += calculateSize(event);
            }

            putMemSize.getAndAdd(size);
        }
        profiling(data, OP.PUT);
        // tell other threads that store is not empty
        notEmpty.signal();
    }

    @Override
    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkUnGetSlotAt((LogPosition) start, batchSize))
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal(); // propagate to non-interrupted thread
                throw ie;
            }

            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                if (checkUnGetSlotAt((LogPosition) start, batchSize)) {
                    return doGet(start, batchSize);
                }
                if (nanos <= 0) {
                    // 如果时间到了，有多少取多少
                    return doGet(start, batchSize);
                }
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    // propagate to non-interrupted thread
                    notEmpty.signal();
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    private Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;

        long current = getSequence.get();
        long maxAbleSequence = putSequence.get();
        long next = current;
        long end = current;
        // 如果startPosition为null，说明是第一次，默认+1处理
        // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
        if (startPosition == null || !startPosition.getPosition().isIncluded()) {
            next = next + 1;
        }

        if (current >= maxAbleSequence) {
            return new Events<>();
        }

        Events<Event> result = new Events<>();
        List<Event> entryList = result.getEvents();
        long memsize = 0;
        if (batchMode.isItemSize()) {
            end = Math.min((next + batchSize - 1), maxAbleSequence);
            // 提取数据并返回
            for (; next <= end; next++) {
                Event event = entries[getIndex(next)];
                if (ddlIsolation && isDdl(event.getEventType())) {
                    // 如果是ddl隔离，直接返回
                    if (entryList.size() == 0) {
                        entryList.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entryList.add(event);
                }
            }
        } else {
            long maxMemSize = batchSize * bufferMemUnit;
            for (; memsize <= maxMemSize && next <= maxAbleSequence; next++) {
                // 永远保证可以取出第一条的记录，避免死锁
                Event event = entries[getIndex(next)];
                if (ddlIsolation && isDdl(event.getEventType())) {
                    // 如果是ddl隔离，直接返回
                    if (entryList.size() == 0) {
                        entryList.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entryList.add(event);
                    memsize += calculateSize(event);
                    end = next;// 记录end位点
                }
            }

        }

        PositionRange<LogPosition> range = new PositionRange<>();
        result.setPositionRange(range);

        range.setStart(CanalEventUtils.createPosition(entryList.get(0)));
        range.setEnd(CanalEventUtils.createPosition(entryList.get(result.getEvents().size() - 1)));
        range.setEndSeq(end);
        // 记录一下是否存在可以被ack的点

        for (int i = entryList.size() - 1; i >= 0; i--) {
            Event event = entryList.get(i);
            // GTID模式,ack的位点必须是事务结尾,因为下一次订阅的时候mysql会发送这个gtid之后的next,如果在事务头就记录了会丢这最后一个事务
            if ((CanalEntry.EntryType.TRANSACTION_BEGIN == event.getEntryType() && StringUtils.isEmpty(event.getGtId()))
                    || CanalEntry.EntryType.TRANSACTION_END == event.getEntryType() || isDdl(event.getEventType())) {
                // 将事务头/尾设置可被为ack的点
                range.setAck(CanalEventUtils.createPosition(event));
                break;
            }
        }

        if (getSequence.compareAndSet(current, end)) {
            getMemSize.addAndGet(memsize);
            notFull.signal();
            profiling(result.getEvents(), OP.GET);
            return result;
        } else {
            return new Events<>();
        }
    }

    @Override
    public LogPosition getFirstPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long firstSeqeuence = ackSequence.get();
            if (firstSeqeuence == INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {
                // 没有ack过数据
                Event event = entries[getIndex(firstSeqeuence + 1)];
                // 最后一次ack为-1，需要移动到下一条,included = false
                return CanalEventUtils.createPosition(event, false);
            } else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {
                // ack未追上put操作
                Event event = entries[getIndex(firstSeqeuence)];
                // 最后一次ack的位置数据,需要移动到下一条,included = false
                return CanalEventUtils.createPosition(event, false);
            } else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence == putSequence.get()) {
                // 已经追上，store中没有数据
                Event event = entries[getIndex(firstSeqeuence)];
                // 最后一次ack的位置数据，和last为同一条，included = false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public LogPosition getLatestPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long latestSequence = putSequence.get();
            if (latestSequence > INIT_SEQUENCE && latestSequence != ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask];
                // 最后一次写入的数据，最后一条未消费的数据
                return CanalEventUtils.createPosition(event, true);
            } else if (latestSequence > INIT_SEQUENCE && latestSequence == ackSequence.get()) {
                // ack已经追上了put操作
                Event event = entries[(int) putSequence.get() & indexMask];
                // 最后一次写入的数据，included = false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void ack(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    @Override
    public void ack(Position position, Long seqId) throws CanalStoreException {
        cleanUntil(position, seqId);
    }

    @Override
    public void cleanUntil(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    private void cleanUntil(Position position, Long seqId) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long sequence = ackSequence.get();
            long maxSequence = getSequence.get();

            boolean hasMatch = false;
            long memsize = 0;
            // ack没有list，但有已存在的foreach，还是节省一下list的开销
            long localExecTime = 0L;
            int deltaRows = 0;
            if (seqId > 0) {
                maxSequence = seqId;
            }
            for (long next = sequence + 1; next <= maxSequence; next++) {
                Event event = entries[getIndex(next)];
                if (localExecTime == 0 && event.getExecuteTime() > 0) {
                    localExecTime = event.getExecuteTime();
                }
                deltaRows += event.getRowsCount();
                memsize += calculateSize(event);
                if ((seqId < 0 || next == seqId) && CanalEventUtils.checkPosition(event, (LogPosition) position)) {
                    // 找到对应的position，更新ack seq
                    hasMatch = true;

                    if (batchMode.isMemSize()) {
                        ackMemSize.addAndGet(memsize);
                        // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                        for (long index = sequence + 1; index < next; index++) {
                            entries[getIndex(index)] = null;// 设置为null
                        }

                        // 考虑getFirstPosition/getLastPosition会获取最后一次ack的position信息
                        // ack清理的时候只处理entry=null，释放内存
                        Event lastEvent = entries[getIndex(next)];
                        lastEvent.setEntry(null);
                        lastEvent.setRawEntry(null);
                    }

                    if (ackSequence.compareAndSet(sequence, next)) {// 避免并发ack
                        notFull.signal();
                        ackTableRows.addAndGet(deltaRows);
                        if (localExecTime > 0) {
                            ackExecTime.lazySet(localExecTime);
                        }
                        return;
                    }
                }
            }
            if (!hasMatch) {// 找不到对应需要ack的position
                throw new CanalStoreException("no match ack position" + position.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rollback() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            getSequence.set(ackSequence.get());
            getMemSize.set(ackMemSize.get());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void cleanAll() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            putSequence.set(INIT_SEQUENCE);
            getSequence.set(INIT_SEQUENCE);
            ackSequence.set(INIT_SEQUENCE);

            putMemSize.set(0);
            getMemSize.set(0);
            ackMemSize.set(0);
            entries = null;
        } finally {
            lock.unlock();
        }
    }

    // =================== helper method =================

    private long getMinimumGetOrAck() {
        long get = getSequence.get();
        long ack = ackSequence.get();
        return Math.min(ack, get);
    }

    /**
     * 查询是否有空位
     */
    private boolean checkFreeSlotAt(final long sequence) {
        final long wrapPoint = sequence - bufferSize;
        final long minPoint = getMinimumGetOrAck();
        // 刚好追上一轮
        if (wrapPoint > minPoint) {
            return false;
        } else {
            // 在bufferSize模式上，再增加memSize控制
            if (batchMode.isMemSize()) {
                final long memSize = putMemSize.get() - ackMemSize.get();
                return memSize < bufferSize * bufferMemUnit;
            } else {
                return true;
            }
        }
    }

    /**
     * 检查是否存在需要get的数据,并且数量>=batchSize
     */
    private boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        if (batchMode.isItemSize()) {
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
            if (startPosition == null || !startPosition.getPosition().isIncluded()) {
                // 少一条数据
                next = next + 1;
            }
            return current < maxAbleSequence && next + batchSize - 1 <= maxAbleSequence;
        } else {
            // 处理内存大小判断
            long currentSize = getMemSize.get();
            long maxAbleSize = putMemSize.get();
            return maxAbleSize - currentSize >= batchSize * bufferMemUnit;
        }
    }

    private long calculateSize(Event event) {
        // 直接返回binlog中的事件大小
        return event.getRawLength();
    }

    private int getIndex(long sequence) {
        return (int) sequence & indexMask;
    }

    private boolean isDdl(EventType type) {
        return type == EventType.ALTER
                || type == EventType.CREATE
                || type == EventType.ERASE
                || type == EventType.RENAME
                || type == EventType.TRUNCATE
                || type == EventType.C_INDEX
                || type == EventType.D_INDEX;
    }

    private void profiling(List<Event> events, OP op) {
        long localExecTime = 0L;
        int deltaRows = 0;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                if (localExecTime == 0 && e.getExecuteTime() > 0) {
                    localExecTime = e.getExecuteTime();
                }
                deltaRows += e.getRowsCount();
            }
        }
        switch (op) {
            case PUT:
                putTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    putExecTime.lazySet(localExecTime);
                }
                break;
            case GET:
                getTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    getExecTime.lazySet(localExecTime);
                }
                break;
            case ACK:
                ackTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    ackExecTime.lazySet(localExecTime);
                }
                break;
            default:
                break;
        }
    }

    private enum OP {
        PUT, GET, ACK
    }
}
