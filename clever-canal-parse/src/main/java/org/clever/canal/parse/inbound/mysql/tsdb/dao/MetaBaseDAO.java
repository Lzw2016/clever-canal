package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.parse.exception.CanalParseException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据库基础支持类
 */
@SuppressWarnings("WeakerAccess")
@Slf4j
public abstract class MetaBaseDAO {
    /**
     * 是否是H2数据库
     */
    private String productName;
    /**
     * 数据源
     */
    private final DataSource dataSource;

    public MetaBaseDAO(DataSource dataSource) {
        this.dataSource = dataSource;
        try {
            initTable();
        } catch (Exception e) {
            log.warn("初始化数据库脚本失败！", e);
        }
    }

    private void initTable() throws SQLException, IOException {
        String fileName = null;
        if (isH2()) {
            fileName = "clever-canal-parse_MySql.sql";
        } else if (isMariaDB() || isMySQL()) {
            fileName = "clever-canal-parse_H2.sql";
        }
        if (StringUtils.isBlank(fileName)) {
            return;
        }
        String initSql;
        try (InputStream inputStream = this.getClass().getResourceAsStream("/database/" + fileName)) {
            if (inputStream == null) {
                log.warn("读取数据库初始化脚本文件失败！[{}]", fileName);
                return;
            }
            initSql = StringUtils.join(IOUtils.readLines(inputStream, StandardCharsets.UTF_8), "\n");
        }
        if (StringUtils.isBlank(initSql)) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            // TODO 判断是否已经初始化完成了
            try (Statement statement = connection.createStatement()) {
                statement.execute(initSql);
            }
        }
    }

    private void initProductName() throws SQLException {
        if (StringUtils.isNotBlank(productName)) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            productName = connection.getMetaData().getDatabaseProductName();
        }
    }

    /**
     * 判断是否是H2数据库
     */
    protected boolean isH2() throws SQLException {
        initProductName();
        return StringUtils.containsIgnoreCase(productName, "H2");
    }

    /**
     * 判断是否是MySQL数据库
     */
    protected boolean isMySQL() throws SQLException {
        initProductName();
        return StringUtils.containsIgnoreCase(productName, "MySQL");
    }

    /**
     * 判断是否是MariaDB数据库
     */
    protected boolean isMariaDB() throws SQLException {
        initProductName();
        return StringUtils.containsIgnoreCase(productName, "MariaDB");
    }

    /**
     * 通用的SQL执行器
     *
     * @param sql         预编译sql
     * @param jdbcExecute 执行处理函数
     * @param <T>         返回数据类型
     */
    protected <T> T execute(String sql, JdbcExecute<T> jdbcExecute) {
        if (StringUtils.isBlank(sql) || jdbcExecute == null) {
            return null;
        }
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                return jdbcExecute.execute(preparedStatement);
            } catch (Exception e) {
                throw new CanalParseException("操作数据库异常", e);
            }
        } catch (Exception e) {
            throw new CanalParseException("操作数据库异常", e);
        }
    }

    public interface JdbcExecute<T> {
        T execute(PreparedStatement preparedStatement) throws SQLException;
    }
}
