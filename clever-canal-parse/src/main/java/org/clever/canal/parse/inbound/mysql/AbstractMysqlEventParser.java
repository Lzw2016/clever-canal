package org.clever.canal.parse.inbound.mysql;

import lombok.Getter;
import lombok.Setter;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.filter.aviater.AviaterRegexFilter;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.dbsync.binlog.LogEvent;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.inbound.AbstractEventParser;
import org.clever.canal.parse.inbound.BinlogParser;
import org.clever.canal.parse.inbound.MultiStageCoprocessor;
import org.clever.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import org.clever.canal.parse.inbound.mysql.tsdb.*;
import org.clever.canal.protocol.position.EntryPosition;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"WeakerAccess"})
public abstract class AbstractMysqlEventParser extends AbstractEventParser<LogEvent> {
    protected static final long BINLOG_START_OFFSET = 4L;

    // ================================================================================================================================= 表结构的时间序列存储(TsDb)
    /**
     * 是否启用TsDb
     */
    protected boolean enableTsDb = false;
    /**
     * 生成快照的时间间隔(单位：小时)
     */
    @Setter
    @Getter
    protected int tsDbSnapshotInterval = 24;
    /**
     * 快照过期时间(单位：小时)
     */
    @Setter
    @Getter
    protected int tsDbSnapshotExpire = 360;
    /**
     * 表结构的时间序列存储实现
     */
    protected TableMetaTsDb tableMetaTsDb;
    /**
     * DatabaseTableMeta 创建工厂
     */
    @Setter
    protected TableMetaTsDbFactory tableMetaTsDbFactory = DefaultTableMetaTsDbFactory.Instance;
    /**
     * 存储TableMeta的数据源配置信息
     */
    @Setter
    protected TableMetaDataSourceConfig dataSourceConfig;

    // ================================================================================================================================= 配置信息
    /**
     * 连接的编码信息
     */
    @Getter
    @Setter
    protected byte connectionCharsetNumber = (byte) 33;
    /**
     * 连接的编码
     */
    @Getter
    @Setter
    protected Charset connectionCharset = StandardCharsets.UTF_8;
    /**
     * 是否过滤DCL (GRANT：授权 | ROLLBACK [WORK] TO [SAVEPOINT]：回退到某一点 | COMMIT [WORK]：提交)
     */
    @Getter
    @Setter
    protected boolean filterQueryDcl = false;
    /**
     * 是否过滤DML (插入：INSERT | 更新：UPDATE | 删除：DELETE)
     */
    @Getter
    @Setter
    protected boolean filterQueryDml = false;
    /**
     * 是否过滤DDL (CREATE TABLE(表)/VIEW(视图)/INDEX(索引)/SYN(同义词)/CLUSTER(簇))
     */
    @Getter
    @Setter
    protected boolean filterQueryDdl = false;
    /**
     * 是否过滤数据行Rows
     */
    @Getter
    @Setter
    protected boolean filterRows = false;
    /**
     * 是否过滤 Table Error
     */
    @Getter
    @Setter
    protected boolean filterTableError = false;
    /**
     * 是否使用 Druid DDL Filter
     */
    @Getter
    @Setter
    protected boolean useDruidDdlFilter = true;

    // ================================================================================================================================= 配置信息
    /**
     * 接收到的binlog数据字节数(单位: )
     */
    @Getter
    protected final AtomicLong receivedBinlogBytes = new AtomicLong(0L);
    /**
     * binlog事情发布阻塞时间(单位: )
     */
    @Getter
    private final AtomicLong eventsPublishBlockingTime = new AtomicLong(0L);

    @Override
    protected BinlogParser<LogEvent> buildParser() {
        LogEventConvert convert = new LogEventConvert();
        if (eventFilter != null && eventFilter instanceof AviaterRegexFilter) {
            convert.setNameFilter((AviaterRegexFilter) eventFilter);
        }
        if (eventBlackFilter != null && eventBlackFilter instanceof AviaterRegexFilter) {
            convert.setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
        }
        convert.setFieldFilterMap(getFieldFilterMap());
        convert.setFieldBlackFilterMap(getFieldBlackFilterMap());
        convert.setCharset(connectionCharset);
        convert.setFilterQueryDcl(filterQueryDcl);
        convert.setFilterQueryDml(filterQueryDml);
        convert.setFilterQueryDdl(filterQueryDdl);
        convert.setFilterRows(filterRows);
        convert.setFilterTableError(filterTableError);
        convert.setUseDruidDdlFilter(useDruidDdlFilter);
        return convert;
    }

    @Override
    public void setEventFilter(CanalEventFilter<String> eventFilter) {
        super.setEventFilter(eventFilter);
        // 触发一下filter变更
        if (eventFilter instanceof AviaterRegexFilter) {
            if (binlogParser instanceof LogEventConvert) {
                ((LogEventConvert) binlogParser).setNameFilter((AviaterRegexFilter) eventFilter);
            }
            if (tableMetaTsDb != null && tableMetaTsDb instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTsDb).setFilter(eventFilter);
            }
        }
    }

    @Override
    public void setEventBlackFilter(CanalEventFilter<String> eventBlackFilter) {
        super.setEventBlackFilter(eventBlackFilter);
        // 触发一下filter变更
        if (eventBlackFilter instanceof AviaterRegexFilter) {
            if (binlogParser instanceof LogEventConvert) {
                ((LogEventConvert) binlogParser).setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
            }
            if (tableMetaTsDb != null && tableMetaTsDb instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTsDb).setBlackFilter(eventBlackFilter);
            }
        }
    }

    @Override
    public void setFieldFilter(String fieldFilter) {
        super.setFieldFilter(fieldFilter);
        // 触发一下filter变更
        if (binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setFieldFilterMap(getFieldFilterMap());
        }
        if (tableMetaTsDb != null && tableMetaTsDb instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTsDb).setFieldFilterMap(getFieldFilterMap());
        }
    }

    @Override
    public void setFieldBlackFilter(String fieldBlackFilter) {
        super.setFieldBlackFilter(fieldBlackFilter);
        // 触发一下filter变更
        if (binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }
        if (tableMetaTsDb != null && tableMetaTsDb instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTsDb).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }
    }

    /**
     * 回滚到指定位点
     */
    @Override
    protected boolean processTableMeta(EntryPosition position) {
        if (tableMetaTsDb != null) {
            if (position.getTimestamp() == null || position.getTimestamp() <= 0) {
                throw new CanalParseException("use GtId and TableMeta TsDb should be config timestamp > 0");
            }
            return tableMetaTsDb.rollback(position);
        }
        return true;
    }

    @Override
    public void start() throws CanalParseException {
        if (enableTsDb) {
            if (tableMetaTsDb == null) {
                synchronized (CanalEventParser.class) {
                    try {
                        // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
                        System.setProperty("canal.instance.destination", destination);
                        // 初始化
                        tableMetaTsDb = tableMetaTsDbFactory.build(destination, dataSourceConfig);
                    } finally {
                        System.clearProperty("canal.instance.destination");
                    }
                }
            }
        }
        super.start();
    }

    @Override
    public void stop() throws CanalParseException {
        if (enableTsDb) {
            tableMetaTsDbFactory.destroy(destination);
            tableMetaTsDb = null;
        }
        super.stop();
    }

    @Override
    protected MultiStageCoprocessor buildMultiStageCoprocessor() {
        MysqlMultiStageCoprocessor mysqlMultiStageCoprocessor = new MysqlMultiStageCoprocessor(parallelBufferSize, parallelThreadSize, (LogEventConvert) binlogParser, transactionBuffer, destination);
        mysqlMultiStageCoprocessor.setEventsPublishBlockingTime(eventsPublishBlockingTime);
        return mysqlMultiStageCoprocessor;
    }

    public void setEnableTsDb(boolean enableTsDb) {
        this.enableTsDb = enableTsDb;
        if (this.enableTsDb) {
            if (tableMetaTsDb == null) {
                // 初始化
                tableMetaTsDb = tableMetaTsDbFactory.build(destination, dataSourceConfig);
            }
        }
    }
}
