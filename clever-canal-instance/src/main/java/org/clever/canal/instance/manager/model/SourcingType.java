package org.clever.canal.instance.manager.model;

/**
 * 数据源类型
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:09 <br/>
 */
@SuppressWarnings("unused")
public enum SourcingType {
    /**
     * mysql DB
     */
    MYSQL,
    /**
     * localBinLog
     */
    LOCAL_BINLOG,
    /**
     * oracle DB
     */
    ORACLE,
    /**
     * RDS mysql DB
     */
    RDS_MYSQL,
}