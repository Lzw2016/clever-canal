package org.clever.canal.parse.inbound.mysql;

import org.apache.commons.lang3.StringUtils;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.inbound.ErosaConnection;
import org.clever.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import org.clever.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import org.clever.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import org.clever.canal.parse.index.CanalLogPositionManager;
import org.clever.canal.parse.support.AuthenticationInfo;
import org.clever.canal.protocol.position.EntryPosition;
import org.clever.canal.protocol.position.LogPosition;

import java.io.IOException;

/**
 * 基于本地binlog文件的复制
 */
@SuppressWarnings({"WeakerAccess", "DuplicatedCode", "unused"})
public class LocalBinlogEventParser extends AbstractMysqlEventParser implements CanalEventParser {

    // 数据库信息
    protected AuthenticationInfo masterInfo;
    protected EntryPosition masterPosition;     // binlog信息
    protected MysqlConnection metaConnection;   // 查询meta信息的链接
    protected TableMetaCache tableMetaCache;    // 对应meta

    protected String directory;
    protected boolean needWait = false;
    protected int bufferSize = 16 * 1024;

    public LocalBinlogEventParser() {
        // this.runningInfo = new AuthenticationInfo();
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        return buildLocalBinLogConnection();
    }

    @Override
    protected void preDump(ErosaConnection connection) {
        metaConnection = buildMysqlConnection();
        try {
            metaConnection.connect();
        } catch (IOException e) {
            throw new CanalParseException(e);
        }
        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setConnection(metaConnection);
            ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
            ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
            ((DatabaseTableMeta) tableMetaTSDB).setSnapshotInterval(tsdbSnapshotInterval);
            ((DatabaseTableMeta) tableMetaTSDB).setSnapshotExpire(tsdbSnapshotExpire);
            tableMetaTSDB.init(destination);
        }
        tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
        ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
    }

    @Override
    protected void afterDump(ErosaConnection connection) {
        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector().getAddress(), e);
            }
        }
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }
        super.start();
    }

    @Override
    public void stop() {
        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector().getAddress(), e);
            }
        }
        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }
        super.stop();
    }

    private ErosaConnection buildLocalBinLogConnection() {
        LocalBinLogConnection connection = new LocalBinLogConnection();
        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        connection.setNeedWait(this.needWait);
        return connection;
    }

    private MysqlConnection buildMysqlConnection() {
        MysqlConnection connection = new MysqlConnection(runningInfo.getAddress(), runningInfo.getUsername(), runningInfo.getPassword(), connectionCharsetNumber, runningInfo.getDefaultDatabaseName());
        connection.getConnector().setReceiveBufferSize(64 * 1024);
        connection.getConnector().setSendBufferSize(64 * 1024);
        connection.getConnector().setSoTimeout(30 * 1000);
        connection.setCharset(connectionCharset);
        return connection;
    }

    @Override
    protected EntryPosition findStartPosition(ErosaConnection connection) {
        // 处理逻辑
        // 1. 首先查询上一次解析成功的最后一条记录
        // 2. 存在最后一条记录，判断一下当前记录是否发生过主备切换
        //    a. 无机器切换，直接返回
        //    b. 存在机器切换，按最后一条记录的stamptime进行查找
        // 3. 不存在最后一条记录，则从默认的位置开始启动
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录
            EntryPosition entryPosition = masterPosition;
            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null) {
                    return new EntryPosition(entryPosition.getTimestamp());
                }
            } else {
                if (entryPosition.getPosition() != null) {
                    // 如果指定binlogName + offest，直接返回
                    return entryPosition;
                } else {
                    return new EntryPosition(entryPosition.getTimestamp());
                }
            }
        } else {
            return logPosition.getPosition();
        }
        return null;
    }

    // ========================= setter / getter =========================

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setMasterPosition(EntryPosition masterPosition) {
        this.masterPosition = masterPosition;
    }

    public void setMasterInfo(AuthenticationInfo masterInfo) {
        this.masterInfo = masterInfo;
    }

    public boolean isNeedWait() {
        return needWait;
    }

    public void setNeedWait(boolean needWait) {
        this.needWait = needWait;
    }
}
