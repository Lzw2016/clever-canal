package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 表结构记录表快照表
 */
@Data
public class MetaSnapshotDO implements Serializable {
    /**
     * 主键
     */
    private Long id;
    /**
     * 创建时间
     */
    private Date gmtCreate;
    /**
     * 修改时间
     */
    private Date gmtModified;
    /**
     * 通道名称
     */
    private String destination;
    /**
     * binlog文件名
     */
    private String binlogFile;
    /**
     * binlog偏移量
     */
    private Long binlogOffset;
    /**
     * binlog节点id
     */
    private String binlogMasterId;
    /**
     * binlog应用的时间戳
     */
    private Long binlogTimestamp;
    /**
     * 表结构数据
     */
    private String data;
    /**
     * 额外的扩展信息
     */
    private String extra;

}
