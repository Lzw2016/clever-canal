package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Date;

/**
 * MetaSnapshot DAO 操作
 */
public class MetaSnapshotDAO extends MetaBaseDAO {
    private static final String INSERT = " insert into meta_snapshot " +
            "     (gmt_create, gmt_modified, destination, binlog_file, binlog_offset, binlog_master_id, binlog_timestamp, data, extra) " +
            " values " +
            "     (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE = " update meta_snapshot set " +
            "     gmt_modified = now()," +
            "     binlog_file = ?, " +
            "     binlog_offset = ?, " +
            "     binlog_master_id = ?, " +
            "     binlog_timestamp = ?, " +
            "     data = ?, " +
            "     extra = ? " +
            "where destination = ? " +
            "    and binlog_timestamp=0";

    private static final String Find_By_Timestamp = " select" +
            "     id, gmt_create, gmt_modified, destination, binlog_file, binlog_offset, binlog_master_id, binlog_timestamp, data, extra " +
            " from meta_snapshot " +
            " where destination = ? " +
            "     and binlog_timestamp < ? " +
            " order by binlog_timestamp desc, id desc " +
            " limit 1";

    private static final String Delete_By_Name = "delete from meta_snapshot where destination = ?";

    private static final String Delete_By_Timestamp = "delete from meta_snapshot where destination = ? and binlog_timestamp < ? and binlog_timestamp > 0";

    public static final String Exists = "select count(1) from meta_snapshot where destination=? and binlog_master_id=? and binlog_file=? and binlog_offset=?";

    public MetaSnapshotDAO(DataSource dataSource) {
        super(dataSource);
    }

    @SuppressWarnings("UnusedReturnValue")
    public int insert(MetaSnapshotDO metaSnapshot) {
        int count = execute(Exists, preparedStatement -> {
            preparedStatement.setString(1, metaSnapshot.getDestination());
            preparedStatement.setString(2, metaSnapshot.getBinlogMasterId());
            preparedStatement.setString(3, metaSnapshot.getBinlogFile());
            preparedStatement.setLong(4, metaSnapshot.getBinlogOffset());
            ResultSet resultSet = preparedStatement.executeQuery();
            int countTmp = 0;
            if (resultSet.next()) {
                countTmp = resultSet.getInt(1);
            }
            return countTmp;
        });
        if (count >= 1) {
            return 0;
        }
        return execute(INSERT, preparedStatement -> {
            preparedStatement.setString(1, metaSnapshot.getDestination());
            preparedStatement.setString(2, metaSnapshot.getBinlogFile());
            preparedStatement.setLong(3, metaSnapshot.getBinlogOffset());
            preparedStatement.setString(4, metaSnapshot.getBinlogMasterId());
            preparedStatement.setLong(5, metaSnapshot.getBinlogTimestamp());
            preparedStatement.setString(6, metaSnapshot.getData());
            preparedStatement.setString(7, metaSnapshot.getExtra());
            return preparedStatement.executeUpdate();
        });
    }

    public int update(MetaSnapshotDO metaSnapshot) {
        return execute(UPDATE, preparedStatement -> {
            preparedStatement.setString(1, metaSnapshot.getBinlogFile());
            preparedStatement.setLong(2, metaSnapshot.getBinlogOffset());
            preparedStatement.setString(3, metaSnapshot.getBinlogMasterId());
            preparedStatement.setLong(4, metaSnapshot.getBinlogTimestamp());
            preparedStatement.setString(5, metaSnapshot.getData());
            preparedStatement.setString(6, metaSnapshot.getExtra());
            preparedStatement.setString(7, metaSnapshot.getDestination());
            return preparedStatement.executeUpdate();
        });
    }

    @SuppressWarnings("DuplicatedCode")
    public MetaSnapshotDO findByTimestamp(String destination, Long timestamp) {
        return execute(Find_By_Timestamp, preparedStatement -> {
            preparedStatement.setString(1, destination);
            preparedStatement.setLong(2, timestamp == null ? 0L : timestamp);
            ResultSet resultSet = preparedStatement.executeQuery();
            MetaSnapshotDO metaSnapshot = null;
            if (resultSet.next()) {
                metaSnapshot = new MetaSnapshotDO();
                metaSnapshot.setId(resultSet.getLong(1));
                metaSnapshot.setGmtCreate(new Date(resultSet.getDate(2).getTime()));
                metaSnapshot.setGmtModified(new Date(resultSet.getDate(3).getTime()));
                metaSnapshot.setDestination(resultSet.getString(4));
                metaSnapshot.setBinlogFile(resultSet.getString(5));
                metaSnapshot.setBinlogOffset(resultSet.getLong(6));
                metaSnapshot.setBinlogMasterId(resultSet.getString(7));
                metaSnapshot.setBinlogTimestamp(resultSet.getLong(8));
                metaSnapshot.setData(resultSet.getString(9));
                metaSnapshot.setExtra(resultSet.getString(10));
            }
            return metaSnapshot;
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
    public int deleteByTimestamp(String destination, long interval) {
        long timestamp = System.currentTimeMillis() - interval * 1000;
        return execute(Delete_By_Timestamp, preparedStatement -> {
            preparedStatement.setString(1, destination);
            preparedStatement.setLong(2, timestamp);
            return preparedStatement.executeUpdate();
        });
    }
}
