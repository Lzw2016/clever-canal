package org.clever.canal.sink;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.sink.entry.group.GroupEventSink;
import org.clever.canal.sink.exception.CanalSinkException;

import java.net.InetSocketAddress;

/**
 * event事件消费者
 *
 * <pre>
 * 1. 剥离filter/sink为独立的两个动作，方便在快速判断数据是否有效
 * </pre>
 */
public interface CanalEventSink<T> extends CanalLifeCycle {
    /**
     * 提交数据
     */
    boolean sink(T event, InetSocketAddress remoteAddress, String destination) throws CanalSinkException, InterruptedException;

    /**
     * 中断消费，比如解析模块发生了切换，想临时中断当前的merge请求，清理对应的上下文状态，可见{@linkplain GroupEventSink}
     */
    void interrupt();
}
