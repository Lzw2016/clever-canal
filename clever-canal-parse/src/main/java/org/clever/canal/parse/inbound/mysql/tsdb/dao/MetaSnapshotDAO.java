package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import java.util.HashMap;

/**
 * canal数据的存储 TODO lzw
 */
@SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "UnusedReturnValue", "unused"})
public class MetaSnapshotDAO extends MetaBaseDAO {

    public Long insert(MetaSnapshotDO snapshotDO) {
//        return (Long) getSqlMapClientTemplate().insert("meta_snapshot.insert", snapshotDO);
        return 0L;
    }

    public Long update(MetaSnapshotDO snapshotDO) {
//        return (Long) getSqlMapClientTemplate().insert("meta_snapshot.update", snapshotDO);
        return 0L;
    }

    public MetaSnapshotDO findByTimestamp(String destination, Long timestamp) {
        HashMap<String, Object> params = new HashMap<>();
        params.put("timestamp", timestamp == null ? 0L : timestamp);
        params.put("destination", destination);
//        return (MetaSnapshotDO) getSqlMapClientTemplate().queryForObject("meta_snapshot.findByTimestamp", params);
        return null;
    }

    public Integer deleteByName(String destination) {
        HashMap<String, Object> params = new HashMap<>();
        params.put("destination", destination);
//        return getSqlMapClientTemplate().delete("meta_snapshot.deleteByName", params);
        return 0;
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByTimestamp(String destination, int interval) {
        HashMap<String, Object> params = new HashMap<>();
        long timestamp = System.currentTimeMillis() - interval * 1000;
        params.put("timestamp", timestamp);
        params.put("destination", destination);
//        return getSqlMapClientTemplate().delete("meta_snapshot.deleteByTimestamp", params);
        return 0;
    }

    protected void initDao() throws Exception {
        initTable("meta_snapshot");
    }

}
