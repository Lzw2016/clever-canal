package org.clever.canal.parse.inbound;

/**
 * 解析binlog的异常处理
 */
public interface ParserExceptionHandler {

    void handle(Throwable e);
}
