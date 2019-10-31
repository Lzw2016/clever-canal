package org.clever.canal.meta;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.meta.exception.CanalMetaManagerException;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.position.Position;
import org.clever.canal.protocol.position.PositionRange;

import java.util.List;
import java.util.Map;

/**
 * meta信息管理器
 */
@SuppressWarnings("unused")
public interface CanalMetaManager extends CanalLifeCycle {

    /**
     * 增加一个 client订阅 <br/>
     * 如果 client已经存在，则不做任何修改
     */
    void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 判断是否订阅
     */
    boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 取消client订阅
     */
    void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 获取 cursor 游标
     */
    Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 更新 cursor 游标
     */
    void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException;

    /**
     * 根据指定的destination列出当前所有的clientIdentity信息
     */
    List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException;

    /**
     * 获得该client的第一个位置的position范围数据
     */
    PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 获得该client最新的一个位置的position范围数据(最后一个位置)
     */
    PositionRange getLatestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 插入batch数据(position范围数据)，返回自动生成的唯一的batchId
     */
    Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException;

    /**
     * 指定batchId，插入batch数据(position范围数据)
     */
    void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId) throws CanalMetaManagerException;

    /**
     * 根据唯一batchId，查找对应的 Position范围数据
     */
    PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException;

    /**
     * 对一个batch的Ack(确认)
     */
    PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException;

    /**
     * 查询当前的所有batch信息
     */
    Map<Long, PositionRange> listAllBatches(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    /**
     * 清除对应的batch信息
     */
    void clearAllBatches(ClientIdentity clientIdentity) throws CanalMetaManagerException;
}
