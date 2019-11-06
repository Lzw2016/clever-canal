package org.clever.canal.client;

import org.clever.canal.protocol.FlatMessage;
import org.clever.canal.protocol.Message;
import org.clever.canal.protocol.exception.CanalClientException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * canal MQ数据操作客户端
 *
 * <pre>
 * 1. canal server写入MQ消息，考虑性能会合并多条数据写入为一个MQ消息，一个Message对应一个MQ消息
 * 2. canal client消费MQ消息，因为client性能会弱于server的写入，MQ数据获取时会拿到堆积的多条MQ消息，会拿到List<Message>
 * 3. client的ack/rollback，都是和MQ直接交互，不存在对应的batchId概念
 * </pre>
 */
public interface CanalMQConnector extends CanalConnector {

    /**
     * 获取数据，自动进行确认，设置timeout时间直到拿到数据为止
     *
     * <pre>
     * 该方法返回的条件：
     *  a. 如果timeout=0，有多少取多少，不会阻塞等待
     *  b. 如果timeout不为0，尝试阻塞对应的超时时间，直到拿到数据就返回
     * </pre>
     */
    List<Message> getList(Long timeout, TimeUnit unit) throws CanalClientException;

    /**
     * 获取数据，设置timeout时间直到拿到数据为止
     *
     * <pre>
     * 该方法返回的条件：
     *  a. 如果timeout=0，有多少取多少，不会阻塞等待
     *  b. 如果timeout不为0，尝试阻塞对应的超时时间，直到拿到数据就返回
     * </pre>
     */
    List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException;

    /**
     * 获取数据，自动进行确认，设置timeout时间直到拿到数据为止
     *
     * <pre>
     * 该方法返回的条件：
     *  a. 如果timeout=0，有多少取多少，不会阻塞等待
     *  b. 如果timeout不为0，尝试阻塞对应的超时时间，直到拿到数据就返回
     * </pre>
     */
    List<FlatMessage> getFlatList(Long timeout, TimeUnit unit) throws CanalClientException;

    /**
     * 获取数据，设置timeout时间直到拿到数据为止
     *
     * <pre>
     * 该方法返回的条件：
     *  a. 如果timeout=0，有多少取多少，不会阻塞等待
     *  b. 如果timeout不为0，尝试阻塞对应的超时时间，直到拿到数据就返回
     * </pre>
     */
    List<FlatMessage> getFlatListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException;

    /**
     * 消费确认。
     */
    void ack() throws CanalClientException;

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    void rollback() throws CanalClientException;
}
