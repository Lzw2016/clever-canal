package org.clever.canal.filter;

import org.clever.canal.filter.exception.CanalFilterException;

/**
 * 数据过滤机制
 */
public interface CanalEventFilter<T> {

    /**
     * 匹配字符串event
     *
     * @param event 需要过滤的对象，数据库表名称使用 ${schema}.${tableName} 匹配
     */
    boolean filter(T event) throws CanalFilterException;
}
