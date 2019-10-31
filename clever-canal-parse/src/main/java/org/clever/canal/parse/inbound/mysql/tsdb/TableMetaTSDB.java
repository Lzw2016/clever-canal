package org.clever.canal.parse.inbound.mysql.tsdb;

import org.clever.canal.parse.inbound.TableMeta;
import org.clever.canal.protocol.position.EntryPosition;

import java.util.Map;

/**
 * 表结构的时间序列存储
 */
public interface TableMetaTSDB {

    /**
     * 初始化
     */
    boolean init(String destination);

    /**
     * 销毁资源
     */
    void destory();

    /**
     * 获取当前的表结构
     */
    TableMeta find(String schema, String table);

    /**
     * 添加ddl到时间序列库中
     */
    boolean apply(EntryPosition position, String schema, String ddl, String extra);

    /**
     * 回滚到指定位点的表结构
     */
    boolean rollback(EntryPosition position);

    /**
     * 生成快照内容
     */
    Map<String/* schema */, String> snapshot();
}
