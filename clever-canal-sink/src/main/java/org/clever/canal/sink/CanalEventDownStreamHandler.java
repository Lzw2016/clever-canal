package org.clever.canal.sink;

import org.clever.canal.common.CanalLifeCycle;

/**
 * 处理下sink时的数据流
 */
public interface CanalEventDownStreamHandler<T> extends CanalLifeCycle {

    /**
     * 提交到store之前做一下处理，允许替换Event
     */
    T before(T events);

    /**
     * store处于full后，retry时处理做一下处理
     */
    T retry(T events);

    /**
     * 提交store成功后做一下处理
     */
    T after(T events);
}
