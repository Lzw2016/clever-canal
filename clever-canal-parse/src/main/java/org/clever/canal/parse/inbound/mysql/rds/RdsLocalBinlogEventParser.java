package org.clever.canal.parse.inbound.mysql.rds;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.clever.canal.common.utils.Assert;
import org.clever.canal.parse.CanalEventParser;
import org.clever.canal.parse.dbsync.binlog.LogEvent;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.exception.PositionNotFoundException;
import org.clever.canal.parse.exception.ServerIdNotMatchException;
import org.clever.canal.parse.inbound.ErosaConnection;
import org.clever.canal.parse.inbound.mysql.LocalBinLogConnection;
import org.clever.canal.parse.inbound.mysql.LocalBinlogEventParser;
import org.clever.canal.parse.inbound.mysql.rds.data.BinlogFile;
import org.clever.canal.protocol.position.EntryPosition;
import org.clever.canal.protocol.position.LogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * 基于rds binlog备份文件的复制
 */
@SuppressWarnings({"WeakerAccess", "ResultOfMethodCallIgnored", "unused"})
public class RdsLocalBinlogEventParser extends LocalBinlogEventParser implements CanalEventParser<LogEvent>, LocalBinLogConnection.FileParserListener {
    private final Logger logger = LoggerFactory.getLogger(RdsLocalBinlogEventParser.class);

    private String url;                // openApi地址
    private String accessKey;          // 云账号的ak
    private String secretKey;          // 云账号sk
    private String instanceId;         // rds实例id
    private Long startTime;
    private Long endTime;
    private BinlogDownloadQueue binlogDownloadQueue;
    private ParseFinishListener finishListener;
    private int batchFileSize;

    public RdsLocalBinlogEventParser() {
    }

    public void start() throws CanalParseException {
        try {
            Assert.notNull(accessKey);
            Assert.notNull(secretKey);
            Assert.notNull(instanceId);
            Assert.notNull(url);
            Assert.notNull(directory);
            if (endTime == null) {
                endTime = System.currentTimeMillis();
            }
            EntryPosition entryPosition = findStartPosition(null);
            if (entryPosition == null) {
                throw new PositionNotFoundException("position not found!");
            }
            Long startTimeInMill = entryPosition.getTimestamp();
            if (startTimeInMill == null || startTimeInMill <= 0) {
                throw new PositionNotFoundException("position timestamp is empty!");
            }
            startTime = startTimeInMill;
            List<BinlogFile> binlogFiles = RdsBinlogOpenApi.listBinlogFiles(url, accessKey, secretKey, instanceId, new Date(startTime), new Date(endTime));
            if (binlogFiles.isEmpty()) {
                throw new CanalParseException("start timestamp : " + startTimeInMill + " binlog files is empty");
            }
            binlogDownloadQueue = new BinlogDownloadQueue(binlogFiles, batchFileSize, directory);
            binlogDownloadQueue.silenceDownload();
            needWait = true;
            // try to download one file,use to test server id
            binlogDownloadQueue.tryOne();
        } catch (Throwable e) {
            logger.error("download binlog failed", e);
            throw new CanalParseException(e);
        }
        setParserExceptionHandler(this::handleMysqlParserException);
        super.start();
    }

    private void handleMysqlParserException(Throwable throwable) {
        if (throwable instanceof ServerIdNotMatchException) {
            logger.error("server id not match, try download another rds binlog!");
            binlogDownloadQueue.notifyNotMatch();
            try {
                binlogDownloadQueue.cleanDir();
                binlogDownloadQueue.tryOne();
                binlogDownloadQueue.prepare();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            try {
                binlogDownloadQueue.execute(() -> {
                    RdsLocalBinlogEventParser.super.stop();
                    RdsLocalBinlogEventParser.super.start();
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        ErosaConnection connection = super.buildErosaConnection();
        if (connection instanceof LocalBinLogConnection) {
            LocalBinLogConnection localBinLogConnection = (LocalBinLogConnection) connection;
            localBinLogConnection.setNeedWait(true);
            localBinLogConnection.setServerId(serverId);
            localBinLogConnection.setParserListener(this);
        }
        return connection;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        if (StringUtils.isNotEmpty(url)) {
            this.url = url;
        }
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    @Override
    public void onFinish(String fileName) {
        try {
            binlogDownloadQueue.downOne();
            File needDeleteFile = new File(directory + File.separator + fileName);
            if (needDeleteFile.exists()) {
                needDeleteFile.delete();
            }
            // 处理下logManager位点问题
            LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
            Long timestamp = 0L;
            if (logPosition != null && logPosition.getPosition() != null) {
                timestamp = logPosition.getPosition().getTimestamp();
                EntryPosition position = logPosition.getPosition();
                LogPosition newLogPosition = new LogPosition();
                String journalName = position.getJournalName();
                int sepIdx = journalName.indexOf(".");
                String fileIndex = journalName.substring(sepIdx + 1);
                int index = NumberUtils.toInt(fileIndex) + 1;
                String newJournalName = journalName.substring(0, sepIdx) + "." + StringUtils.leftPad(String.valueOf(index), fileIndex.length(), "0");
                newLogPosition.setPosition(new EntryPosition(newJournalName, 4L, position.getTimestamp(), position.getServerId()));
                newLogPosition.setIdentity(logPosition.getIdentity());
                logPositionManager.persistLogPosition(destination, newLogPosition);
            }
            if (binlogDownloadQueue.isLastFile(fileName)) {
                logger.info("last file : " + fileName + " , timestamp : " + timestamp + " , all file parse complete, switch to mysql parser!");
                finishListener.onFinish();
                return;
            } else {
                logger.info("parse local binlog file : " + fileName + " , timestamp : " + timestamp + " , try the next binlog !");
            }
            binlogDownloadQueue.prepare();
        } catch (Exception e) {
            logger.error("prepare download binlog file failed!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        this.binlogDownloadQueue.release();
        super.stop();
    }

    public void setFinishListener(ParseFinishListener finishListener) {
        this.finishListener = finishListener;
    }

    public interface ParseFinishListener {
        void onFinish();
    }

    public void setBatchFileSize(int batchFileSize) {
        this.batchFileSize = batchFileSize;
    }
}
