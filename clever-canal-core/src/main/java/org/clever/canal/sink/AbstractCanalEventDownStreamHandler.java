package org.clever.canal.sink;

import org.clever.canal.common.AbstractCanalLifeCycle;

/**
 * 默认的实现
 */
@SuppressWarnings("unused")
public class AbstractCanalEventDownStreamHandler<T> extends AbstractCanalLifeCycle implements CanalEventDownStreamHandler<T> {

    public T before(T events) {
        return events;
    }

    public T retry(T events) {
        return events;
    }

    public T after(T events) {
        return events;
    }
}
