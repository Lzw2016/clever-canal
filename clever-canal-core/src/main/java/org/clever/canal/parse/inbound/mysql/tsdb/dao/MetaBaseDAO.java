package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

//import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

/**
 * TODO lzw
 */
@SuppressWarnings("deprecation")
public class MetaBaseDAO  {
//    extends SqlMapClientDaoSupport

    protected boolean isH2 = false;

    protected void initTable(String tableName) throws Exception {
//        Connection conn = null;
//        InputStream input = null;
//        try {
//            DataSource dataSource = getDataSource();
//            conn = dataSource.getConnection();
//            String name = "mysql";
//            isH2 = isH2(conn);
//            if (isH2) {
//                name = "h2";
//            }
//            input = Thread.currentThread()
//                .getContextClassLoader()
//                .getResourceAsStream("ddl/" + name + "/" + tableName + ".sql");
//            if (input == null) {
//                return;
//            }
//
//            String sql = StringUtils.join(IOUtils.readLines(input), "\n");
//            Statement stmt = conn.createStatement();
//            stmt.execute(sql);
//            stmt.close();
//        } catch (Throwable e) {
//            logger.warn("init " + tableName + " failed", e);
//        } finally {
//            IOUtils.closeQuietly(input);
//            if (conn != null) {
//                conn.close();
//            }
//        }
    }

    private boolean isH2(Connection conn) throws SQLException {
        String product = conn.getMetaData().getDatabaseProductName();
        return StringUtils.containsIgnoreCase(product, "H2");
    }
}