package org.clever.canal.parse;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.SQLException;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/04 17:00 <br/>
 */
@Slf4j
public class DataSourceTest {

    private HikariDataSource getHikariDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        // 默认配置
        hikariConfig.setPoolName("DatabaseTableMetaSource");
        hikariConfig.setAutoCommit(true);
        hikariConfig.getDataSourceProperties().setProperty("serverTimezone", "GMT+8");
        hikariConfig.getDataSourceProperties().setProperty("useUnicode", "GMT+8");
        hikariConfig.getDataSourceProperties().setProperty("characterEncoding", "UTF-8");
        hikariConfig.getDataSourceProperties().setProperty("zeroDateTimeBehavior", "convert_to_null");
        hikariConfig.getDataSourceProperties().setProperty("useSSL", "false");
        // 读取配置
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariConfig.setJdbcUrl("jdbc:mysql://mysql.msvc.top:3306/clever-canal");
        hikariConfig.setUsername("clever-canal");
        hikariConfig.setPassword("lizhiwei");

//        hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
//        hikariConfig.setJdbcUrl("jdbc:mariadb://127.0.0.1:3306/clever-nashorn");
//        hikariConfig.setUsername("root");
//        hikariConfig.setPassword("lizhiwei");
        return new HikariDataSource(hikariConfig);
    }

    @Test
    public void t1() throws SQLException {
        HikariDataSource dataSource = getHikariDataSource();
        log.info("#### --> {}", dataSource.getConnection().getMetaData().getDatabaseProductName());
        dataSource.close();
        dataSource.close();
        dataSource.close();
        dataSource.close();
        dataSource.close();
    }
}
