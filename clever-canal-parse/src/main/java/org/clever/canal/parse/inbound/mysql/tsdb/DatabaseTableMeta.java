package org.clever.canal.parse.inbound.mysql.tsdb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastsql.sql.repository.Schema;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.inbound.TableMeta;
import org.clever.canal.parse.inbound.TableMeta.FieldMeta;
import org.clever.canal.parse.inbound.mysql.MysqlConnection;
import org.clever.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import org.clever.canal.parse.inbound.mysql.ddl.DdlResult;
import org.clever.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import org.clever.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDAO;
import org.clever.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDO;
import org.clever.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDAO;
import org.clever.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDO;
import org.clever.canal.protocol.position.EntryPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * 使用 内存+数据库方式维护DDL <br />
 * <pre>
 *     1.定时把内存中的数据刷到数据库快照表
 *     2.只要内存数据变更就把数据刷到数据库历史记录表
 * </pre>
 */
public class DatabaseTableMeta implements TableMetaTSDB {
    private static Logger logger = LoggerFactory.getLogger(DatabaseTableMeta.class);

    public static final EntryPosition INIT_POSITION = new EntryPosition("0", 0L, -2L, -1L);
    private static final Pattern pattern = Pattern.compile("Duplicate entry '.*' for key '*'");
    private static final Pattern h2Pattern = Pattern.compile("Unique index or primary key violation");
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "[scheduler-table-meta-snapshot]");
        thread.setDaemon(true);
        return thread;
    });

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private String destination;
    private MemoryTableMeta memoryTableMeta;
    private ScheduledFuture<?> scheduleSnapshotFuture;
    private EntryPosition lastPosition;
    /**
     * 是否有新的DDL变化SQL
     */
    private boolean hasNewDdl;
    /**
     * 用于查询数据库表结构信息
     */
    @Setter
    private volatile MysqlConnection connection;
    /**
     * 白名单过滤器
     */
    @Setter
    private CanalEventFilter<String> filter;
    /**
     * 黑名单过滤器
     */
    @Setter
    private CanalEventFilter<String> blackFilter;
    /**
     * 操作 MetaHistory
     */
    @Setter
    private MetaHistoryDAO metaHistoryDAO;
    /**
     * 操作 MetaSnapshot
     */
    @Setter
    private MetaSnapshotDAO metaSnapshotDAO;
    /**
     * 生成快照的时间间隔(单位：小时) <br/>
     * 默认1天
     */
    @Setter
    private int snapshotInterval = 24;
    /**
     * 快照过期时间(单位：小时) <br/>
     * 默认15天
     */
    @Setter
    private int snapshotExpire = 360;

    public DatabaseTableMeta() {
    }

    @Override
    public boolean init(final String destination) {
        if (initialized.compareAndSet(false, true)) {
            this.destination = destination;
            this.memoryTableMeta = new MemoryTableMeta();
            // 24小时生成一份snapshot
            if (snapshotInterval > 0) {
                scheduleSnapshotFuture = scheduler.scheduleWithFixedDelay(() -> {
                            boolean applyResult = false;
                            // 保存表结构快照到数据库
                            try {
                                MDC.put("destination", destination);
                                applyResult = applySnapshotToDB(lastPosition, false);
                                logger.debug("schedule applySnapshotToDB state: {}", applyResult ? "successful" : "failed");
                            } catch (Throwable e) {
                                logger.error("schedule applySnapshotToDB failed", e);
                            } finally {
                                MDC.remove("destination");
                            }
                            // 如果表结构快照保存成功，则设置之前的数据已过期
                            try {
                                MDC.put("destination", destination);
                                if (applyResult) {
                                    int count = snapshotExpire(TimeUnit.HOURS.toSeconds(snapshotExpire));
                                    logger.debug("schedule snapshotExpire successful | 数据量: {}", count);
                                }
                            } catch (Throwable e) {
                                logger.error("schedule snapshotExpire failed", e);
                            } finally {
                                MDC.remove("destination");
                            }
                        },
                        snapshotInterval,
                        snapshotInterval,
                        TimeUnit.HOURS
                );
            }
        }
        return true;
    }

    @Override
    public void destroy() {
        if (memoryTableMeta != null) {
            memoryTableMeta.destroy();
        }
        if (connection != null) {
            try {
                connection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", connection.getConnector().getAddress(), e);
            }
        }
        if (scheduleSnapshotFuture != null) {
            scheduleSnapshotFuture.cancel(false);
        }
    }

    @Override
    public TableMeta find(String schema, String table) {
        lock.readLock().lock();
        try {
            return memoryTableMeta.find(schema, table);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        // 首先记录到内存结构
        lock.writeLock().lock();
        try {
            if (memoryTableMeta.apply(position, schema, ddl, extra)) {
                this.lastPosition = position;
                this.hasNewDdl = true;
                // 同步每次变更给远程做历史记录
                return applyHistoryToDB(position, schema, ddl, extra);
            } else {
                throw new RuntimeException("apply to memory is failed");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean rollback(EntryPosition position) {
        // 每次rollback需要重新构建一次memory data
        this.memoryTableMeta.destroy();
        this.memoryTableMeta = new MemoryTableMeta();
        boolean flag = false;
        // 从数据库构建快照
        EntryPosition snapshotPosition = buildMemFromSnapshot(position);
        if (snapshotPosition != null) {
            boolean isSuccess = applyHistoryOnMemory(snapshotPosition, position);
            if (isSuccess) {
                flag = true;
            }
        }
        if (!flag) {
            // 如果没有任何数据，则为初始化状态，全量dump一份关注的表
            if (dumpTableMeta(connection, filter)) {
                // 记录一下snapshot结果,方便快速恢复
                flag = applySnapshotToDB(INIT_POSITION, true);
            }
        }
        return flag;
    }

    @Override
    public Map<String, String> snapshot() {
        return memoryTableMeta.snapshot();
    }

    /**
     * 保存数据库表结构快照
     *
     * @param position binlog位置信息
     * @param init     是否是第一次保存(初始化保存)
     */
    private boolean applySnapshotToDB(EntryPosition position, boolean init) {
        // 获取一份快照
        Map<String, String> schemaDdlList;
        lock.readLock().lock();
        try {
            if (!init && !hasNewDdl) {
                // 如果是持续构建,则识别一下是否有DDL变更过,如果没有就忽略了
                return false;
            }
            this.hasNewDdl = false;
            schemaDdlList = memoryTableMeta.snapshot();
        } finally {
            lock.readLock().unlock();
        }
        MemoryTableMeta tmpMemoryTableMeta = new MemoryTableMeta();
        for (Map.Entry<String, String> entry : schemaDdlList.entrySet()) {
            tmpMemoryTableMeta.apply(position, entry.getKey(), entry.getValue(), null);
        }
        // 基于临时内存对象进行对比
        boolean compareAll = true;
        for (Schema schema : tmpMemoryTableMeta.getRepository().getSchemas()) {
            for (String table : schema.showTables()) {
                String fullName = schema + "." + table;
                if (blackFilter == null || !blackFilter.filter(fullName)) {
                    if (filter == null || filter.filter(fullName)) {
                        // issue : https://github.com/alibaba/canal/issues/1168
                        // 在生成snapshot时重新过滤一遍
                        if (!compareTableMetaDbAndMemory(connection, tmpMemoryTableMeta, schema.getName(), table)) {
                            compareAll = false;
                        }
                    }
                }
            }
        }
        if (compareAll) {
            // 内存中的表结构与数据库当前的一致
            MetaSnapshotDO snapshotDO = createMetaSnapshot(position, schemaDdlList);
            try {
                metaSnapshotDAO.insert(snapshotDO);
            } catch (Throwable e) {
                if (isUkDuplicateException(e)) {
                    // 忽略掉重复的位点
                    logger.warn("dup apply snapshot use position : " + position + " , just ignore");
                } else {
                    throw new CanalParseException("apply failed caused by : " + e.getMessage(), e);
                }
            }
            return true;
        } else {
            logger.error("compare failed , check log");
        }
        return false;
    }

    /**
     * 新建表结构记录表快照数据
     *
     * @param position      binlog位置信息
     * @param schemaDdlList 表结构快照数据
     */
    private MetaSnapshotDO createMetaSnapshot(EntryPosition position, Map<String, String> schemaDdlList) {
        MetaSnapshotDO snapshot = new MetaSnapshotDO();
        snapshot.setDestination(destination);
        snapshot.setBinlogFile(position.getJournalName());
        snapshot.setBinlogOffset(position.getPosition());
        snapshot.setBinlogMasterId(String.valueOf(position.getServerId()));
        snapshot.setBinlogTimestamp(position.getTimestamp());
        snapshot.setData(JSON.toJSONString(schemaDdlList));
        snapshot.setGmtCreate(new Date());
        snapshot.setGmtModified(new Date());
        // snapshot.setExtra();
        return snapshot;
    }

    /**
     * 记录表结构历史信息
     *
     * @param position binlog位置信息
     * @param schema   schema
     * @param ddl      ddl SQL
     * @param extra    扩展数据
     */
    private boolean applyHistoryToDB(EntryPosition position, String schema, String ddl, String extra) {
        MetaHistoryDO metaHistory = createMetaHistory(position, schema, ddl, extra);
        try {
            // 会建立唯一约束,解决:
            // 1. 重复的binlog file + offset
            // 2. 重复的masterId+timestamp
            metaHistoryDAO.insert(metaHistory);
        } catch (Throwable e) {
            if (isUkDuplicateException(e)) {
                // 忽略掉重复的位点
                logger.warn("dup apply for sql : " + ddl);
            } else {
                throw new CanalParseException("apply history to db failed caused by : " + e.getMessage(), e);
            }
        }
        return true;
    }

    /**
     * 新建表结构变化明细数据
     */
    private MetaHistoryDO createMetaHistory(EntryPosition position, String schema, String ddl, String extra) {
        MetaHistoryDO metaHistory = new MetaHistoryDO();
        metaHistory.setGmtCreate(new Date());
        metaHistory.setGmtModified(new Date());
        metaHistory.setDestination(destination);
        metaHistory.setBinlogFile(position.getJournalName());
        metaHistory.setBinlogOffset(position.getPosition());
        metaHistory.setBinlogMasterId(String.valueOf(position.getServerId()));
        metaHistory.setBinlogTimestamp(position.getTimestamp());
        metaHistory.setUseSchema(schema);
        // 待补充
        List<DdlResult> ddlResults = DruidDdlParser.parse(ddl, schema);
        if (ddlResults.size() > 0) {
            DdlResult ddlResult = ddlResults.get(0);
            metaHistory.setSqlSchema(ddlResult.getSchemaName());
            metaHistory.setSqlTable(ddlResult.getTableName());
            metaHistory.setSqlText(ddl);
            metaHistory.setSqlType(ddlResult.getType().name());
            metaHistory.setExtra(extra);
        }
        return metaHistory;
    }

    /**
     * 比较当前数据库表结构 与 内存中的表结构数据是否一致
     *
     * @param connection      数据库连接
     * @param memoryTableMeta 内存中的表结构数据
     * @param schema          schema名称
     * @param table           table名称
     */
    private boolean compareTableMetaDbAndMemory(MysqlConnection connection, MemoryTableMeta memoryTableMeta, final String schema, final String table) {
        TableMeta tableMetaFromMem = memoryTableMeta.find(schema, table);
        TableMeta tableMetaFromDB = new TableMeta();
        tableMetaFromDB.setSchema(schema);
        tableMetaFromDB.setTable(table);
        String createDDL = null;
        try {
            ResultSetPacket packet = connection.query("show create table " + getFullName(schema, table));
            if (packet.getFieldValues().size() > 1) {
                createDDL = packet.getFieldValues().get(1);
                tableMetaFromDB.setFields(TableMetaCache.parseTableMeta(schema, table, packet));
            }
        } catch (Throwable e) {
            try {
                // retry for broke pipe, see:
                // https://github.com/alibaba/canal/issues/724
                connection.reconnect();
                ResultSetPacket packet = connection.query("show create table " + getFullName(schema, table));
                if (packet.getFieldValues().size() > 1) {
                    createDDL = packet.getFieldValues().get(1);
                    tableMetaFromDB.setFields(TableMetaCache.parseTableMeta(schema, table, packet));
                }
            } catch (IOException e1) {
                if (e.getMessage().contains("errorNumber=1146")) {
                    logger.error("table not exist in db , pls check :" + getFullName(schema, table) + " , mem : " + tableMetaFromMem);
                    return false;
                }
                throw new CanalParseException(e);
            }
        }
        boolean result = compareTableMeta(tableMetaFromMem, tableMetaFromDB);
        if (!result) {
            logger.error("pls submit github issue, show create table ddl:" + createDDL + " , compare failed . \n db : " + tableMetaFromDB + " \n mem : " + tableMetaFromMem);
        }
        return result;
    }

    /**
     * 则设置${expireTimestamp}秒之前的快照数据已过期
     *
     * @param expireTimestamp ${expireTimestamp}秒 (单位：秒)
     */
    private int snapshotExpire(long expireTimestamp) {
        return metaSnapshotDAO.deleteByTimestamp(destination, expireTimestamp);
    }


    /**
     * 初始化的时候dump一下表结构(全量dump一份关注的表)
     */
    private boolean dumpTableMeta(MysqlConnection connection, final CanalEventFilter<String> filter) {
        try {
            ResultSetPacket packet = connection.query("show databases");
            List<String> schemas = new ArrayList<>(packet.getFieldValues());
            for (String schema : schemas) {
                // filter views
                packet = connection.query("show full tables from `" + schema + "` where Table_type = 'BASE TABLE'");
                List<String> tables = new ArrayList<>();
                for (String table : packet.getFieldValues()) {
                    if ("BASE TABLE".equalsIgnoreCase(table)) {
                        continue;
                    }
                    String fullName = schema + "." + table;
                    if (blackFilter == null || !blackFilter.filter(fullName)) {
                        if (filter == null || filter.filter(fullName)) {
                            tables.add(table);
                        }
                    }
                }
                if (tables.isEmpty()) {
                    continue;
                }
                StringBuilder sql = new StringBuilder();
                for (String table : tables) {
                    sql.append("show create table `").append(schema).append("`.`").append(table).append("`;");
                }
                List<ResultSetPacket> packets = connection.queryMulti(sql.toString());
                for (ResultSetPacket onePacket : packets) {
                    if (onePacket.getFieldValues().size() > 1) {
                        String oneTableCreateSql = onePacket.getFieldValues().get(1);
                        memoryTableMeta.apply(INIT_POSITION, schema, oneTableCreateSql, null);
                    }
                }
            }
            return true;
        } catch (IOException e) {
            throw new CanalParseException(e);
        }
    }

    /**
     * 从数据库加载表结构快照到内存
     */
    private EntryPosition buildMemFromSnapshot(EntryPosition position) {
        try {
            MetaSnapshotDO snapshotDO = metaSnapshotDAO.findByTimestamp(destination, position.getTimestamp());
            if (snapshotDO == null) {
                return null;
            }
            String binlogFile = snapshotDO.getBinlogFile();
            Long binlogOffset = snapshotDO.getBinlogOffset();
            String binlogMasterId = snapshotDO.getBinlogMasterId();
            Long binlogTimestamp = snapshotDO.getBinlogTimestamp();
            EntryPosition snapshotPosition = new EntryPosition(
                    binlogFile,
                    binlogOffset == null ? 0L : binlogOffset,
                    binlogTimestamp == null ? 0L : binlogTimestamp,
                    Long.valueOf(binlogMasterId == null ? "-2" : binlogMasterId)
            );
            // data存储为Map<String,String>，每个分库一套建表
            String sqlData = snapshotDO.getData();
            JSONObject jsonObj = JSON.parseObject(sqlData);
            for (Map.Entry entry : jsonObj.entrySet()) {
                String schema = Objects.toString(entry.getKey(), StringUtils.EMPTY);
                String ddl = Objects.toString(entry.getValue(), StringUtils.EMPTY);
                // 记录到内存
                if (!memoryTableMeta.apply(snapshotPosition, schema, ddl, null)) {
                    return null;
                }
            }
            return snapshotPosition;
        } catch (Throwable e) {
            throw new CanalParseException("apply failed caused by : " + e.getMessage(), e);
        }
    }

    /**
     * 从数据库加载表结构历史到内存
     */
    private boolean applyHistoryOnMemory(EntryPosition position, EntryPosition rollbackPosition) {
        try {
            List<MetaHistoryDO> metaHistoryDOList = metaHistoryDAO.findByTimestamp(destination, position.getTimestamp(), rollbackPosition.getTimestamp());
            if (metaHistoryDOList == null) {
                return true;
            }
            for (MetaHistoryDO metaHistoryDO : metaHistoryDOList) {
                String binlogFile = metaHistoryDO.getBinlogFile();
                Long binlogOffset = metaHistoryDO.getBinlogOffset();
                String binlogMasterId = metaHistoryDO.getBinlogMasterId();
                Long binlogTimestamp = metaHistoryDO.getBinlogTimestamp();
                String useSchema = metaHistoryDO.getUseSchema();
                String sqlData = metaHistoryDO.getSqlText();
                EntryPosition snapshotPosition = new EntryPosition(binlogFile, binlogOffset == null ? 0L : binlogOffset, binlogTimestamp == null ? 0L : binlogTimestamp, Long.valueOf(binlogMasterId == null ? "-2" : binlogMasterId));
                // 如果是同一秒内,对比一下history的位点，如果比期望的位点要大，忽略之
                if (snapshotPosition.getTimestamp() > rollbackPosition.getTimestamp()) {
                    continue;
                } else if (rollbackPosition.getServerId().equals(snapshotPosition.getServerId()) && snapshotPosition.compareTo(rollbackPosition) > 0) {
                    continue;
                }
                // 记录到内存
                if (!memoryTableMeta.apply(snapshotPosition, useSchema, sqlData, null)) {
                    return false;
                }
            }
            return metaHistoryDOList.size() > 0;
        } catch (Throwable e) {
            throw new CanalParseException("apply failed", e);
        }
    }

    private String getFullName(String schema, String table) {
        return '`' + schema + '`' + '.' + '`' + table + '`';
    }

    /**
     * 比较两个 TableMeta 是否一致
     */
    private static boolean compareTableMeta(TableMeta source, TableMeta target) {
        if (!StringUtils.equalsIgnoreCase(source.getSchema(), target.getSchema())) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(source.getTable(), target.getTable())) {
            return false;
        }
        List<FieldMeta> sourceFields = source.getFields();
        List<FieldMeta> targetFields = target.getFields();
        if (sourceFields.size() != targetFields.size()) {
            return false;
        }
        /*
         * MySQL DDL的一些默认行为:
         * <pre>
         * 1. Timestamp类型的列在第一次添加时，未指定默认值会默认为CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
         * 2. Timestamp类型的列在第二次时，必须指定默认值
         * 3. BLOB和TEXT类型不存在NULL、NOT NULL属性
         * 4. 部分数据类型是synonyms，实际show create table时会转成对应类型
         * 5. 非BLOB和TEXT类型在默认未指定NULL、NOT NULL时，默认default null
         * 6. 在列变更时，不仅变更列名数据类型，同时各个约束中列名也会变更，同时如果约束中包含key length，则变更后的数据类型不应违背key length的约束（有长度的应大于key length；BLOB、TEXT应有key length；可以在存在key length情况下变更为无key length的数据类型，约束中key length忽略；等等）
         * 7. 字符集每列（char类、enum、set）默认保存，指定使用指定的，未指定使用表默认的，不受修改表默认字符集而改变，同表默认时，字符集显示省略
         * 8. 新建表默认innodb引擎，latin1字符集
         * 9. BLOB、TEXT会根据给定长度自动转换为对应的TINY、MEDIUM，LONG类型，长度和字符集也有关
         * 10. unique约束在没有指定索引名是非幂等的，会自动以约束索引第一个列名称命名，同时以_2,_3这种形式添加后缀
         * </pre>
         */
        for (int i = 0; i < sourceFields.size(); i++) {
            FieldMeta sourceField = sourceFields.get(i);
            FieldMeta targetField = targetFields.get(i);
            if (!StringUtils.equalsIgnoreCase(sourceField.getColumnName(), targetField.getColumnName())) {
                return false;
            }
            // if (!StringUtils.equalsIgnoreCase(sourceField.getColumnType(), targetField.getColumnType())) {
            //     return false;
            // }
            // https://github.com/alibaba/canal/issues/1100
            // 支持一下 int vs int(10)
            if ((sourceField.isUnsigned() && !targetField.isUnsigned())
                    || (!sourceField.isUnsigned() && targetField.isUnsigned())) {
                return false;
            }
            String sourceColumnType = StringUtils.removeEndIgnoreCase(sourceField.getColumnType(), "zerofill").trim();
            String targetColumnType = StringUtils.removeEndIgnoreCase(targetField.getColumnType(), "zerofill").trim();
            String sign = sourceField.isUnsigned() ? "unsigned" : "signed";
            sourceColumnType = StringUtils.removeEndIgnoreCase(sourceColumnType, sign).trim();
            targetColumnType = StringUtils.removeEndIgnoreCase(targetColumnType, sign).trim();
            boolean columnTypeCompare;
            columnTypeCompare = StringUtils.containsIgnoreCase(sourceColumnType, targetColumnType);
            columnTypeCompare |= StringUtils.containsIgnoreCase(targetColumnType, sourceColumnType);
            if (!columnTypeCompare) {
                // 去掉精度参数再对比一次
                sourceColumnType = synonymsType(StringUtils.substringBefore(sourceColumnType, "(")).trim();
                targetColumnType = synonymsType(StringUtils.substringBefore(targetColumnType, "(")).trim();
                columnTypeCompare = StringUtils.containsIgnoreCase(sourceColumnType, targetColumnType);
                columnTypeCompare |= StringUtils.containsIgnoreCase(targetColumnType, sourceColumnType);
                if (!columnTypeCompare) {
                    return false;
                }
            }
            // if (!StringUtils.equalsIgnoreCase(sourceField.getDefaultValue(), targetField.getDefaultValue())) {
            //     return false;
            // }
            // BLOB, TEXT, GEOMETRY or JSON默认都是nullable，可以忽略比较，但比较了也是对齐
            // noinspection StatementWithEmptyBody (压制IDE警告)
            if (StringUtils.containsIgnoreCase(sourceColumnType, "timestamp") || StringUtils.containsIgnoreCase(targetColumnType, "timestamp")) {
                // timestamp可能会加default current_timestamp默认值,忽略对比nullable
            } else {
                if (sourceField.isNullable() != targetField.isNullable()) {
                    return false;
                }
            }
            // mysql会有一种处理,针对show create只有uk没有pk时，会在desc默认将uk当做pk
            boolean isSourcePkOrUk = sourceField.isKey() || sourceField.isUnique();
            boolean isTargetPkOrUk = targetField.isKey() || targetField.isUnique();
            if (isSourcePkOrUk != isTargetPkOrUk) {
                return false;
            }
        }
        return true;
    }

    /**
     * <pre>
     * synonyms处理
     * 1. BOOL/BOOLEAN => TINYINT
     * 2. DEC/NUMERIC/FIXED => DECIMAL
     * 3. INTEGER => INT
     * </pre>
     */
    private static String synonymsType(String originType) {
        if (StringUtils.equalsIgnoreCase(originType, "bool") || StringUtils.equalsIgnoreCase(originType, "boolean")) {
            return "tinyint";
        } else if (StringUtils.equalsIgnoreCase(originType, "dec") || StringUtils.equalsIgnoreCase(originType, "numeric") || StringUtils.equalsIgnoreCase(originType, "fixed")) {
            return "decimal";
        } else if (StringUtils.equalsIgnoreCase(originType, "integer")) {
            return "int";
        } else if (StringUtils.equalsIgnoreCase(originType, "real") || StringUtils.equalsIgnoreCase(originType, "double precision")) {
            return "double";
        }
        // BLOB、TEXT会根据给定长度自动转换为对应的TINY、MEDIUM，LONG类型，长度和字符集也有关，统一按照blob对比
        if (StringUtils.equalsIgnoreCase(originType, "tinyblob") || StringUtils.equalsIgnoreCase(originType, "mediumblob") || StringUtils.equalsIgnoreCase(originType, "longblob")) {
            return "blob";
        } else if (StringUtils.equalsIgnoreCase(originType, "tinytext") || StringUtils.equalsIgnoreCase(originType, "mediumtext") || StringUtils.equalsIgnoreCase(originType, "longtext")) {
            return "text";
        }
        return originType;
    }

    /**
     * 违反外键约束时也抛出这种异常，所以这里还要判断包含字符串Duplicate entry
     */
    private boolean isUkDuplicateException(Throwable t) {
        return pattern.matcher(t.getMessage()).find() || h2Pattern.matcher(t.getMessage()).find();
    }


    @SuppressWarnings("unused")
    public void setFieldFilterMap(Map<String, List<String>> fieldFilterMap) {
        // TODO 删除
//        this.fieldFilterMap = fieldFilterMap;
    }

    @SuppressWarnings("unused")
    public void setFieldBlackFilterMap(Map<String, List<String>> fieldBlackFilterMap) {
        // TODO 删除
//        this.fieldBlackFilterMap = fieldBlackFilterMap;
    }
}
