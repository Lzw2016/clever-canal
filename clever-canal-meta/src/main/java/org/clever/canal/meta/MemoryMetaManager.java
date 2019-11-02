package org.clever.canal.meta;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.utils.MigrateMap;
import org.clever.canal.meta.exception.CanalMetaManagerException;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.position.Position;
import org.clever.canal.protocol.position.PositionRange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存版实现
 */
@SuppressWarnings({"WeakerAccess"})
public class MemoryMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {
    /**
     * 通道名称(destination) --> 客户端标识集合(List<ClientIdentity>)
     */
    protected Map<String, List<ClientIdentity>> destinations;
    /**
     * 客户端标识(ClientIdentity) --> 基于单个ClientIdentity的binlog消费位置信息管理
     */
    protected Map<ClientIdentity, MemoryClientIdentityBatch> batches;
    /**
     * 客户端标识(ClientIdentity) --> 客户端当前位置信息(Position)
     */
    protected Map<ClientIdentity, Position> cursors;

    /**
     * 初始化 CanalMetaManager
     */
    @Override
    public void start() {
        super.start();
        batches = MigrateMap.makeComputingMap(MemoryClientIdentityBatch::create);
        cursors = new ConcurrentHashMap<>();
        destinations = MigrateMap.makeComputingMap(destination -> new ArrayList<>());
    }

    /**
     * 清除 CanalMetaManager 的数据
     */
    @Override
    public void stop() {
        super.stop();
        destinations.clear();
        cursors.clear();
        for (MemoryClientIdentityBatch batch : batches.values()) {
            batch.clearPositionRanges();
        }
        batches.clear();
    }

    /**
     * 增加一个 client订阅 <br/>
     * 如果 client已经存在，则不做任何修改
     */
    @Override
    public synchronized void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentities = destinations.get(clientIdentity.getDestination());
        clientIdentities.remove(clientIdentity);
        clientIdentities.add(clientIdentity);
    }

    /**
     * 判断是否订阅
     */
    @Override
    public synchronized boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentities = destinations.get(clientIdentity.getDestination());
        return clientIdentities != null && clientIdentities.contains(clientIdentity);
    }

    /**
     * 取消client订阅
     */
    @Override
    public synchronized void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentities = destinations.get(clientIdentity.getDestination());
        if (clientIdentities != null) {
            clientIdentities.remove(clientIdentity);
        }
    }

    /**
     * 根据指定的destination列出当前所有的clientIdentity信息
     */
    @Override
    public synchronized List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        // fixed issue #657, fixed ConcurrentModificationException
        return Collections.unmodifiableList(new ArrayList<>(destinations.get(destination)));
    }

    /**
     * 获取 cursor 游标
     */
    @Override
    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return cursors.get(clientIdentity);
    }

    /**
     * 更新 cursor 游标
     */
    @Override
    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        cursors.put(clientIdentity, position);
    }

    /**
     * 为 client 产生一个唯一、递增的id
     */
    @Override
    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        return batches.get(clientIdentity).addPositionRange(positionRange);
    }

    /**
     * 指定batchId，插入batch数据
     */
    @Override
    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId) throws CanalMetaManagerException {
        // 添加记录到指定batchId
        batches.get(clientIdentity).addPositionRange(positionRange, batchId);
    }

    /**
     * 对一个batch的Ack(确认)
     */
    @Override
    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).removePositionRange(batchId);
    }

    /**
     * 根据唯一batchId，查找对应的 Position范围数据
     */
    @Override
    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getPositionRange(batchId);
    }

    /**
     * 获得该client的第一个位置的position范围数据
     */
    @Override
    public PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getFirstPositionRange();
    }

    /**
     * 获得该client最新的一个位置的position范围数据(最后一个位置)
     */
    @Override
    public PositionRange getLatestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getLatestPositionRange();
    }

    /**
     * 查询当前的所有batch信息
     */
    @Override
    public Map<Long, PositionRange> listAllBatches(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).listAllPositionRange();
    }

    /**
     * 清除对应的batch信息
     */
    @Override
    public void clearAllBatches(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        batches.get(clientIdentity).clearPositionRanges();
    }

    // ============================

    /**
     * 基于内存的Batch数据存储
     */
    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class MemoryClientIdentityBatch {
        /**
         * 客户端标识(对应客户端)
         */
        private ClientIdentity clientIdentity;
        /**
         * batchId --> position范围
         */
        private Map<Long, PositionRange> batches = new ConcurrentHashMap<>();
        /**
         * 自动递增的BatchId
         */
        private AtomicLong atomicMaxBatchId = new AtomicLong(1);

        public static MemoryClientIdentityBatch create(ClientIdentity clientIdentity) {
            return new MemoryClientIdentityBatch(clientIdentity);
        }

        public MemoryClientIdentityBatch() {
        }

        protected MemoryClientIdentityBatch(ClientIdentity clientIdentity) {
            this.clientIdentity = clientIdentity;
        }

        /**
         * 指定batchId，插入batch数据(position范围数据)
         *
         * @param positionRange 插入batch数据
         * @param batchId       batchId
         */
        public synchronized void addPositionRange(PositionRange positionRange, Long batchId) {
            updateMaxId(batchId);
            batches.put(batchId, positionRange);
        }

        /**
         * 插入batch数据(position范围数据)，返回自动生成的唯一的batchId
         *
         * @param positionRange 插入batch数据
         * @return 自动生成的唯一的batchId
         */
        public synchronized Long addPositionRange(PositionRange positionRange) {
            Long batchId = atomicMaxBatchId.getAndIncrement();
            batches.put(batchId, positionRange);
            return batchId;
        }

        /**
         * 对一个batch的Ack(确认) - 删除对于的范围数据
         */
        public synchronized PositionRange removePositionRange(Long batchId) {
            if (batches.containsKey(batchId)) {
                Long minBatchId = Collections.min(batches.keySet());
                if (!minBatchId.equals(batchId)) {
                    // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
                    throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d", batchId, minBatchId));
                }
                return batches.remove(batchId);
            } else {
                return null;
            }
        }

        /**
         * 根据唯一batchId，查找对应的 Position范围数据
         */
        public synchronized PositionRange getPositionRange(Long batchId) {
            return batches.get(batchId);
        }

        /**
         * 获得该client最新的一个位置(最后一个位置)
         */
        public synchronized PositionRange getLatestPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.max(batches.keySet());
                return batches.get(batchId);
            }
        }

        /**
         * 获得该client的第一个位置的position范围数据
         */
        public synchronized PositionRange getFirstPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.min(batches.keySet());
                return batches.get(batchId);
            }
        }

        /**
         * 查询当前的所有batch信息
         */
        public synchronized Map<Long, PositionRange> listAllPositionRange() {
            Set<Long> batchIdSets = batches.keySet();
            List<Long> batchIds = new ArrayList<>(batchIdSets);
            Collections.sort(new ArrayList<>(batchIds));
            return new HashMap<>(batches);
        }

        /**
         * 清除所有数据
         */
        public synchronized void clearPositionRanges() {
            batches.clear();
        }

        /**
         * 更新batchId到 batchId + 1
         *
         * @param batchId batchId
         */
        private synchronized void updateMaxId(Long batchId) {
            if (atomicMaxBatchId.get() < batchId + 1) {
                atomicMaxBatchId.set(batchId + 1);
            }
        }

        // ============ setter & getter =========

        public ClientIdentity getClientIdentity() {
            return clientIdentity;
        }

        public void setClientIdentity(ClientIdentity clientIdentity) {
            this.clientIdentity = clientIdentity;
        }
    }
}
