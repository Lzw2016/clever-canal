package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * MetaHistory DAO 操作
 */
public class MetaHistoryDAO extends MetaBaseDAO {
    private static final String INSERT = " insert into meta_history " +
            "     (gmt_create, gmt_modified, destination, binlog_file, binlog_offset, binlog_master_id, binlog_timestamp, use_schema, sql_schema, sql_table, sql_text, sql_type, extra) " +
            " values " +
            "     (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String Find_By_Timestamp = " select" +
            "    id, gmt_create, gmt_modified, destination, binlog_file, binlog_offset, binlog_master_id, binlog_timestamp, use_schema, sql_schema, sql_table, sql_text, sql_type, extra" +
            " from meta_history " +
            " where destination = ? " +
            "   and binlog_timestamp >= ? " +
            "   and binlog_timestamp <= ? " +
            " order by binlog_timestamp, id";

    private static final String Delete_By_Name = "delete from meta_history where destination = ?";

    private static final String Delete_By_Timestamp = "delete from meta_history where destination=? and binlog_timestamp < ?";

    public MetaHistoryDAO(DataSource dataSource) {
        super(dataSource);
    }

    @SuppressWarnings("UnusedReturnValue")
    public int insert(MetaHistoryDO metaHistory) {
        return execute(INSERT, preparedStatement -> {
            preparedStatement.setString(1, metaHistory.getDestination());
            preparedStatement.setString(2, metaHistory.getBinlogFile());
            preparedStatement.setLong(3, metaHistory.getBinlogOffset());
            preparedStatement.setString(4, metaHistory.getBinlogMasterId());
            preparedStatement.setLong(5, metaHistory.getBinlogTimestamp());
            preparedStatement.setString(6, metaHistory.getUseSchema());
            preparedStatement.setString(7, metaHistory.getSqlSchema());
            preparedStatement.setString(8, metaHistory.getSqlTable());
            preparedStatement.setString(9, metaHistory.getSqlText());
            preparedStatement.setString(10, metaHistory.getSqlType());
            preparedStatement.setString(11, metaHistory.getExtra());
            return preparedStatement.executeUpdate();
        });
    }

    @SuppressWarnings("DuplicatedCode")
    public List<MetaHistoryDO> findByTimestamp(String destination, Long snapshotTimestamp, Long timestamp) {
        return execute(Find_By_Timestamp, preparedStatement -> {
            preparedStatement.setString(1, destination);
            preparedStatement.setLong(2, snapshotTimestamp);
            preparedStatement.setLong(3, timestamp);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<MetaHistoryDO> list = new ArrayList<>();
            while (resultSet.next()) {
                MetaHistoryDO metaHistory = new MetaHistoryDO();
                metaHistory.setId(resultSet.getLong(1));
                metaHistory.setGmtCreate(new Date(resultSet.getDate(2).getTime()));
                metaHistory.setGmtModified(new Date(resultSet.getDate(3).getTime()));
                metaHistory.setDestination(resultSet.getString(4));
                metaHistory.setBinlogFile(resultSet.getString(5));
                metaHistory.setBinlogOffset(resultSet.getLong(6));
                metaHistory.setBinlogMasterId(resultSet.getString(7));
                metaHistory.setBinlogTimestamp(resultSet.getLong(8));
                metaHistory.setUseSchema(resultSet.getString(9));
                metaHistory.setSqlSchema(resultSet.getString(10));
                metaHistory.setSqlTable(resultSet.getString(11));
                metaHistory.setSqlText(resultSet.getString(12));
                metaHistory.setSqlType(resultSet.getString(13));
                metaHistory.setExtra(resultSet.getString(14));
                list.add(metaHistory);
            }
            return list;
        });
    }

    @SuppressWarnings("unused")
    public int deleteByName(String destination) {
        return execute(Delete_By_Name, preparedStatement -> {
            preparedStatement.setString(1, destination);
            return preparedStatement.executeUpdate();
        });
    }

    /**
     * 删除interval秒之前的数据
     */
    @SuppressWarnings("unused")
    public int deleteByTimestamp(String destination, int interval) {
        long timestamp = System.currentTimeMillis() - interval * 1000;
        return execute(Delete_By_Timestamp, preparedStatement -> {
            preparedStatement.setString(1, destination);
            preparedStatement.setLong(2, timestamp);
            return preparedStatement.executeUpdate();
        });
    }
}
