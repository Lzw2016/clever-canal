package org.clever.canal.instance.manager.model;

import lombok.Data;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * 数据来源描述
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:10 <br/>
 */
@Data
public class DataSourcing implements Serializable {
    private static final long serialVersionUID = -1770648468678085234L;
    /**
     * 数据源类型
     */
    private SourcingType type;
    /**
     * 数据源地址
     */
    private InetSocketAddress dbAddress;

    public DataSourcing(SourcingType type, InetSocketAddress dbAddress) {
        this.type = type;
        this.dbAddress = dbAddress;
    }
}