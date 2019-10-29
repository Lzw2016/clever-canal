package org.clever.canal.sink.entry.group;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 针对group合并的barrier接口，控制多个sink操作的合并处理
 */
public interface GroupBarrier<T> {

    /**
     * 判断当前的数据对象是否允许通过
     */
    void await(T event) throws InterruptedException;

    /**
     * 判断当前的数据对象是否允许通过，带超时控制
     */
    void await(T event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * sink成功，清理对应barrier的状态
     */
    void clear(T event);

    /**
     * 出现切换，发起interrupt，清理对应的上下文
     */
    void interrupt();
}
