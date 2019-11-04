package org.clever.canal.parse.inbound.mysql.tsdb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.HashMap;
import java.util.Map;

/**
 * TableMetaTsDb 创建器默认实现
 */
public class DefaultTableMetaTsDbFactory implements TableMetaTsDbFactory {
    /**
     * destination --> TableMetaTsDb
     */
    private static final Map<String, TableMetaTsDb> Table_Meta_TsDb_Map = new HashMap<>();
    /**
     * 数据源 destination --> DataSource
     */
    private static final Map<String, HikariDataSource> DataSource_Map = new HashMap<>();

    public static final DefaultTableMetaTsDbFactory Instance = new DefaultTableMetaTsDbFactory();

    static {
        // 关闭连接池
        Runtime.getRuntime().addShutdownHook(new Thread(() -> DataSource_Map.values().forEach(HikariDataSource::close)));
    }

    private DefaultTableMetaTsDbFactory() {
    }

    @Override
    public synchronized TableMetaTsDb build(String destination, TableMetaDataSourceConfig dataSourceConfig) {
        return Table_Meta_TsDb_Map.computeIfAbsent(destination, s -> createTableMetaTsDb(destination, dataSourceConfig));
    }

    @Override
    public void destroy(String destination) {
        Table_Meta_TsDb_Map.remove(destination);
        HikariDataSource dataSource = DataSource_Map.get(destination);
        if (dataSource != null) {
            dataSource.close();
            DataSource_Map.remove(destination);
        }
    }

    private synchronized TableMetaTsDb createTableMetaTsDb(String destination, TableMetaDataSourceConfig dataSourceConfig) {
        // TODO 减少 dataSource的创建(重复的就共用?)
        HikariDataSource dataSource = DataSource_Map.computeIfAbsent(destination, s -> {
            HikariConfig hikariConfig = new HikariConfig();
            // 默认配置
            hikariConfig.setPoolName("DatabaseTableMetaSource");
            hikariConfig.setAutoCommit(true);
            hikariConfig.getDataSourceProperties().setProperty("serverTimezone", "GMT+8");
            hikariConfig.getDataSourceProperties().setProperty("useUnicode", "GMT+8");
            hikariConfig.getDataSourceProperties().setProperty("characterEncoding", "UTF-8");
            hikariConfig.getDataSourceProperties().setProperty("zeroDateTimeBehavior", "convert_to_null");
            hikariConfig.getDataSourceProperties().setProperty("useSSL", "false");
            hikariConfig.getDataSourceProperties().setProperty("allowMultiQueries", "true");
            // 读取配置
            hikariConfig.setDriverClassName(dataSourceConfig.getDriverClassName());
            hikariConfig.setJdbcUrl(dataSourceConfig.getUrl());
            hikariConfig.setUsername(dataSourceConfig.getUsername());
            hikariConfig.setPassword(dataSourceConfig.getPassword());
            hikariConfig.setMaximumPoolSize(dataSourceConfig.getMaxPoolSize());
            hikariConfig.setMinimumIdle(dataSourceConfig.getMinIdle());
            hikariConfig.setMaxLifetime(dataSourceConfig.getMaxLifetime());
            hikariConfig.setConnectionTestQuery(dataSourceConfig.getConnectionTestQuery());
            return new HikariDataSource(hikariConfig);
        });
        return new DatabaseTableMeta(dataSource);
    }
}
