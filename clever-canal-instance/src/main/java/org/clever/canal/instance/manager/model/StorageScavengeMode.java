package org.clever.canal.instance.manager.model;

/**
 * EventStore 内存回收模式
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:08 <br/>
 */
@SuppressWarnings("unused")
public enum StorageScavengeMode {
    /**
     * 在存储满的时候触发
     */
    ON_FULL,
    /**
     * 在每次有ack请求时触发
     */
    ON_ACK,
    /**
     * 定时触发，需要外部控制
     */
    ON_SCHEDULE,
    /**
     * 不做任何操作，由外部进行清理
     */
    NO_OP,
}
