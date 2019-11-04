package org.clever.canal.parse.inbound.mysql.tsdb;

/**
 * tableMeta构造器,允许重载实现
 */
public interface TableMetaTSDBFactory {

    /**
     * 代理一下tableMetaTSDB的获取,使用隔离的spring定义
     */
    TableMetaTsDb build(String destination, String springXml);

    void destroy(String destination);
}
