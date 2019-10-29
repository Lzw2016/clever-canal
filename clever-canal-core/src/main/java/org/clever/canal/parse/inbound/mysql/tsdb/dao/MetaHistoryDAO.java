package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * canal数据的存储 TODO lzw
 */
@SuppressWarnings("deprecation")
public class MetaHistoryDAO extends MetaBaseDAO {

    public Long insert(MetaHistoryDO metaDO) {
//        return (Long) getSqlMapClientTemplate().insert("meta_history.insert", metaDO);
        return 0L;
    }

    public List<MetaHistoryDO> findByTimestamp(String destination, Long snapshotTimestamp, Long timestamp) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
        params.put("snapshotTimestamp", snapshotTimestamp == null ? 0L : snapshotTimestamp);
        params.put("timestamp", timestamp == null ? 0L : timestamp);
//        return (List<MetaHistoryDO>) getSqlMapClientTemplate().queryForList("meta_history.findByTimestamp", params);
        return Collections.emptyList();
    }

    public Integer deleteByName(String destination) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        params.put("destination", destination);
//        return getSqlMapClientTemplate().delete("meta_history.deleteByName", params);
        return 0;
    }

    /**
     * 删除interval秒之前的数据
     */
    public Integer deleteByTimestamp(String destination, int interval) {
        HashMap params = Maps.newHashMapWithExpectedSize(2);
        long timestamp = System.currentTimeMillis() - interval * 1000;
        params.put("timestamp", timestamp);
        params.put("destination", destination);
//        return getSqlMapClientTemplate().delete("meta_history.deleteByTimestamp", params);
        return 0;
    }

    protected void initDao() throws Exception {
        initTable("meta_history");
    }
}
