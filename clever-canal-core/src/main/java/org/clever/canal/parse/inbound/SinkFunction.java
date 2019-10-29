package org.clever.canal.parse.inbound;

/**
 * receive parsed bytes , 用于处理要解析的数据块
 */
@SuppressWarnings("unused")
public interface SinkFunction<EVENT> {

    boolean sink(EVENT event);
}
