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

    protected Map<String, List<ClientIdentity>> destinations;
    protected Map<ClientIdentity, MemoryClientIdentityBatch> batches;
    protected Map<ClientIdentity, Position> cursors;

    public void start() {
        super.start();
        batches = MigrateMap.makeComputingMap(MemoryClientIdentityBatch::create);
        cursors = new ConcurrentHashMap<>();
        destinations = MigrateMap.makeComputingMap(destination -> new ArrayList<>());
    }

    public void stop() {
        super.stop();
        destinations.clear();
        cursors.clear();
        for (MemoryClientIdentityBatch batch : batches.values()) {
            batch.clearPositionRanges();
        }
    }

    public synchronized void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        clientIdentitys.remove(clientIdentity);
        clientIdentitys.add(clientIdentity);
    }

    public synchronized boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        return clientIdentitys != null && clientIdentitys.contains(clientIdentity);
    }

    public synchronized void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        if (clientIdentitys != null) {
            clientIdentitys.remove(clientIdentity);
        }
    }

    public synchronized List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        // fixed issue #657, fixed ConcurrentModificationException
        return new ArrayList<>(destinations.get(destination));
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return cursors.get(clientIdentity);
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        cursors.put(clientIdentity, position);
    }

    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        return batches.get(clientIdentity).addPositionRange(positionRange);
    }

    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId)
            throws CanalMetaManagerException {
        batches.get(clientIdentity).addPositionRange(positionRange, batchId);// 添加记录到指定batchId
    }

    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).removePositionRange(batchId);
    }

    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getPositionRange(batchId);
    }

    public PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getLastestPositionRange();
    }

    public PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getFirstPositionRange();
    }

    public Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).listAllPositionRange();
    }

    public void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        batches.get(clientIdentity).clearPositionRanges();
    }

    // ============================

    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class MemoryClientIdentityBatch {

        private ClientIdentity clientIdentity;
        private Map<Long, PositionRange> batches = new ConcurrentHashMap<>();
        private AtomicLong atomicMaxBatchId = new AtomicLong(1);

        public static MemoryClientIdentityBatch create(ClientIdentity clientIdentity) {
            return new MemoryClientIdentityBatch(clientIdentity);
        }

        public MemoryClientIdentityBatch() {

        }

        protected MemoryClientIdentityBatch(ClientIdentity clientIdentity) {
            this.clientIdentity = clientIdentity;
        }

        public synchronized void addPositionRange(PositionRange positionRange, Long batchId) {
            updateMaxId(batchId);
            batches.put(batchId, positionRange);
        }

        public synchronized Long addPositionRange(PositionRange positionRange) {
            Long batchId = atomicMaxBatchId.getAndIncrement();
            batches.put(batchId, positionRange);
            return batchId;
        }

        public synchronized PositionRange removePositionRange(Long batchId) {
            if (batches.containsKey(batchId)) {
                Long minBatchId = Collections.min(batches.keySet());
                if (!minBatchId.equals(batchId)) {
                    // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
                    throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d",
                            batchId,
                            minBatchId));
                }
                return batches.remove(batchId);
            } else {
                return null;
            }
        }

        public synchronized PositionRange getPositionRange(Long batchId) {
            return batches.get(batchId);
        }

        public synchronized PositionRange getLastestPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.max(batches.keySet());
                return batches.get(batchId);
            }
        }

        public synchronized PositionRange getFirstPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.min(batches.keySet());
                return batches.get(batchId);
            }
        }

        public synchronized Map<Long, PositionRange> listAllPositionRange() {
            Set<Long> batchIdSets = batches.keySet();
            List<Long> batchIds = new ArrayList<>(batchIdSets);
            Collections.sort(new ArrayList<>(batchIds));
            return new HashMap<>(batches);
        }

        public synchronized void clearPositionRanges() {
            batches.clear();
        }

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