package org.clever.canal.sink;

import org.clever.canal.common.AbstractCanalLifeCycle;

/**
 * 默认的实现
 */
public class AbstractCanalEventDownStreamHandler<T> extends AbstractCanalLifeCycle implements CanalEventDownStreamHandler<T> {

    @Override
    public T before(T events) {
        return events;
    }

    @Override
    public T retry(T events) {
        return events;
    }

    @Override
    public T after(T events) {
        return events;
    }
}
