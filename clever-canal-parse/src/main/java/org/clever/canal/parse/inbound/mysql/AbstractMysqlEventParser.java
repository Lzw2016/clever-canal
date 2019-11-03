package org.clever.canal.parse.inbound.mysql;

import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.filter.aviater.AviaterRegexFilter;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.inbound.AbstractEventParser;
import org.clever.canal.parse.inbound.BinlogParser;
import org.clever.canal.parse.inbound.MultiStageCoprocessor;
import org.clever.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import org.clever.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import org.clever.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import org.clever.canal.parse.inbound.mysql.tsdb.TableMetaTSDBFactory;
import org.clever.canal.protocol.position.EntryPosition;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"WeakerAccess", "unchecked", "unused"})
public abstract class AbstractMysqlEventParser extends AbstractEventParser {

    protected static final long BINLOG_START_OFFSET = 4L;

    // TODO lzw 初始化 tableMetaTSDBFactory = new DefaultTableMetaTSDBFactory();
    protected TableMetaTSDBFactory tableMetaTSDBFactory;
    protected boolean enableTsDb = false;
    protected int tsDbSnapshotInterval = 24;
    protected int tsDbSnapshotExpire = 360;
    protected String tsDbSpringXml;
    protected TableMetaTSDB tableMetaTSDB;

    // 编码信息
    protected byte connectionCharsetNumber = (byte) 33;
    protected Charset connectionCharset = StandardCharsets.UTF_8;
    protected boolean filterQueryDcl = false;
    protected boolean filterQueryDml = false;
    protected boolean filterQueryDdl = false;
    protected boolean filterRows = false;
    protected boolean filterTableError = false;
    protected boolean useDruidDdlFilter = true;
    // instance received binlog bytes
    protected final AtomicLong receivedBinlogBytes = new AtomicLong(0L);
    private final AtomicLong eventsPublishBlockingTime = new AtomicLong(0L);

    protected BinlogParser buildParser() {
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

    public void setEventFilter(CanalEventFilter eventFilter) {
        super.setEventFilter(eventFilter);
        // 触发一下filter变更
        if (eventFilter instanceof AviaterRegexFilter) {
            if (binlogParser instanceof LogEventConvert) {
                ((LogEventConvert) binlogParser).setNameFilter((AviaterRegexFilter) eventFilter);
            }
            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
            }
        }
    }

    public void setEventBlackFilter(CanalEventFilter eventBlackFilter) {
        super.setEventBlackFilter(eventBlackFilter);
        // 触发一下filter变更
        if (eventBlackFilter instanceof AviaterRegexFilter) {
            if (binlogParser instanceof LogEventConvert) {
                ((LogEventConvert) binlogParser).setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
            }
            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
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
        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFieldFilterMap(getFieldFilterMap());
        }
    }

    @Override
    public void setFieldBlackFilter(String fieldBlackFilter) {
        super.setFieldBlackFilter(fieldBlackFilter);
        // 触发一下filter变更
        if (binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }
        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }
    }

    /**
     * 回滚到指定位点
     */
    protected boolean processTableMeta(EntryPosition position) {
        if (tableMetaTSDB != null) {
            if (position.getTimestamp() == null || position.getTimestamp() <= 0) {
                throw new CanalParseException("use GtId and TableMeta TsDb should be config timestamp > 0");
            }
            return tableMetaTSDB.rollback(position);
        }
        return true;
    }

    public void start() throws CanalParseException {
        if (enableTsDb) {
            if (tableMetaTSDB == null) {
                synchronized (CanalEventParser.class) {
                    try {
                        // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
                        System.setProperty("canal.instance.destination", destination);
                        // 初始化
                        tableMetaTSDB = tableMetaTSDBFactory.build(destination, tsDbSpringXml);
                    } finally {
                        System.setProperty("canal.instance.destination", "");
                    }
                }
            }
        }
        super.start();
    }

    public void stop() throws CanalParseException {
        if (enableTsDb) {
            tableMetaTSDBFactory.destroy(destination);
            tableMetaTSDB = null;
        }
        super.stop();
    }

    protected MultiStageCoprocessor buildMultiStageCoprocessor() {
        MysqlMultiStageCoprocessor mysqlMultiStageCoprocessor = new MysqlMultiStageCoprocessor(parallelBufferSize, parallelThreadSize, (LogEventConvert) binlogParser, transactionBuffer, destination);
        mysqlMultiStageCoprocessor.setEventsPublishBlockingTime(eventsPublishBlockingTime);
        return mysqlMultiStageCoprocessor;
    }

    // ============================ setter / getter =========================

    public void setConnectionCharsetNumber(byte connectionCharsetNumber) {
        this.connectionCharsetNumber = connectionCharsetNumber;
    }

    public void setConnectionCharset(Charset connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public void setConnectionCharset(String connectionCharset) {
        this.connectionCharset = Charset.forName(connectionCharset);
    }

    public void setFilterQueryDcl(boolean filterQueryDcl) {
        this.filterQueryDcl = filterQueryDcl;
    }

    public void setFilterQueryDml(boolean filterQueryDml) {
        this.filterQueryDml = filterQueryDml;
    }

    public void setFilterQueryDdl(boolean filterQueryDdl) {
        this.filterQueryDdl = filterQueryDdl;
    }

    public void setFilterRows(boolean filterRows) {
        this.filterRows = filterRows;
    }

    public void setFilterTableError(boolean filterTableError) {
        this.filterTableError = filterTableError;
    }

    public boolean isUseDruidDdlFilter() {
        return useDruidDdlFilter;
    }

    public void setUseDruidDdlFilter(boolean useDruidDdlFilter) {
        this.useDruidDdlFilter = useDruidDdlFilter;
    }

    public void setEnableTsDb(boolean enableTsDb) {
        this.enableTsDb = enableTsDb;
        if (this.enableTsDb) {
            if (tableMetaTSDB == null) {
                // 初始化
                tableMetaTSDB = tableMetaTSDBFactory.build(destination, tsDbSpringXml);
            }
        }
    }

    public void setTsDbSpringXml(String tsDbSpringXml) {
        this.tsDbSpringXml = tsDbSpringXml;
        if (this.enableTsDb) {
            if (tableMetaTSDB == null) {
                // 初始化
                tableMetaTSDB = tableMetaTSDBFactory.build(destination, tsDbSpringXml);
            }
        }
    }

    public void setTableMetaTSDBFactory(TableMetaTSDBFactory tableMetaTSDBFactory) {
        this.tableMetaTSDBFactory = tableMetaTSDBFactory;
    }

    public AtomicLong getEventsPublishBlockingTime() {
        return this.eventsPublishBlockingTime;
    }

    public AtomicLong getReceivedBinlogBytes() {
        return this.receivedBinlogBytes;
    }

    public int getTsDbSnapshotInterval() {
        return tsDbSnapshotInterval;
    }

    public void setTsDbSnapshotInterval(int tsDbSnapshotInterval) {
        this.tsDbSnapshotInterval = tsDbSnapshotInterval;
    }

    public int getTsDbSnapshotExpire() {
        return tsDbSnapshotExpire;
    }

    public void setTsDbSnapshotExpire(int tsDbSnapshotExpire) {
        this.tsDbSnapshotExpire = tsDbSnapshotExpire;
    }
}
