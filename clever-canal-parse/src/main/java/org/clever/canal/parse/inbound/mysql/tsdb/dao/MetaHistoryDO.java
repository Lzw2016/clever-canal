package org.clever.canal.parse.inbound.mysql.tsdb.dao;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 表结构变化明细表
 */
@Data
public class MetaHistoryDO implements Serializable {
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
     * 执行sql时对应的schema
     */
    private String useSchema;
    /**
     * 对应的schema
     */
    private String sqlSchema;
    /**
     * 对应的table
     */
    private String sqlTable;
    /**
     * 执行的sql
     */
    private String sqlText;
    /**
     * sql类型
     */
    private String sqlType;
    /**
     * 额外的扩展信息
     */
    private String extra;
}