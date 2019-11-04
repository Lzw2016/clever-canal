package org.clever.canal.parse.inbound.mysql.tsdb;

/**
 * TableMetaTsDb 创建器
 */
public interface TableMetaTsDbFactory {

    /**
     * 构建 TableMetaTsDb
     */
    TableMetaTsDb build(String destination, TableMetaDataSourceConfig dataSourceConfig);

    /**
     * 销毁 TableMetaTsDb
     */
    void destroy(String destination);
}
