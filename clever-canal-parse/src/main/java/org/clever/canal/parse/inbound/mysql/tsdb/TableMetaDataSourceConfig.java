package org.clever.canal.parse.inbound.mysql.tsdb;

import lombok.Data;

import java.io.Serializable;

/**
 * 存储TableMeta的数据源配置信息
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/04 16:07 <br/>
 */
@Data
public class TableMetaDataSourceConfig implements Serializable {
    /**
     * 数据库驱动class
     */
    private String driverClassName;
    /**
     * 数据库连接URL
     */
    private String url;
    /**
     * 数据库用户名
     */
    private String username;
    /**
     * 数据库密码
     */
    private String password;
    /**
     * 连接池最大大小
     */
    private int maxPoolSize = 5;
    /**
     * 连接池空闲连接数量
     */
    private int minIdle = 1;
    /**
     * 连接最初存活时间
     */
    private long maxLifetime = 1800000;
    /**
     * 测试连接查询
     */
    private String connectionTestQuery = "SELECT 1";
}
