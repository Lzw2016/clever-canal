package org.clever.canal.filter;

import org.clever.canal.filter.exception.CanalFilterException;

/**
 * 数据过滤机制
 */
public interface CanalEventFilter<T> {

    boolean filter(T event) throws CanalFilterException;
}
