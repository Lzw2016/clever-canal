package org.clever.canal.parse.inbound.mysql;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.parse.dbsync.binlog.*;
import org.clever.canal.parse.dbsync.binlog.event.QueryLogEvent;
import org.clever.canal.parse.driver.mysql.packets.GtIdSet;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.parse.exception.ServerIdNotMatchException;
import org.clever.canal.parse.inbound.ErosaConnection;
import org.clever.canal.parse.inbound.MultiStageCoprocessor;
import org.clever.canal.parse.inbound.SinkFunction;
import org.clever.canal.parse.inbound.mysql.local.BinLogFileQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * local bin log connection (not real connection)
 */
@SuppressWarnings({"unchecked", "WeakerAccess", "unused", "DuplicatedCode"})
public class LocalBinLogConnection implements ErosaConnection {

    private static final Logger logger = LoggerFactory.getLogger(LocalBinLogConnection.class);
    private BinLogFileQueue binlogs = null;
    private boolean needWait;
    private String directory;
    private int bufferSize = 16 * 1024;
    private boolean running = false;
    private long serverId;
    private FileParserListener parserListener;

    public LocalBinLogConnection() {
    }

    public LocalBinLogConnection(String directory, boolean needWait) {
        this.needWait = needWait;
        this.directory = directory;
    }

    @Override
    public void connect() {
        if (this.binlogs == null) {
            this.binlogs = new BinLogFileQueue(this.directory);
        }
        this.running = true;
    }

    @Override
    public void reconnect() {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() {
        this.running = false;
        if (this.binlogs != null) {
            this.binlogs.destroy();
        }
        this.binlogs = null;
        this.running = false;
    }

    public boolean isConnected() {
        return running;
    }

    public void seek(String binlogFileName, Long binlogPosition, String gtid, SinkFunction func) {
    }

    public void dump(String binlogFileName, Long binlogPosition, SinkFunction func) throws IOException {
        File current = new File(directory, binlogFileName);
        try (FileLogFetcher fetcher = new FileLogFetcher(bufferSize)) {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogFileName, binlogPosition));
            while (running) {
                boolean needContinue = true;
                LogEvent event;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }
                    if (serverId != 0 && event.getServerId() != serverId) {
                        throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                    }
                    if (!func.sink(event)) {
                        needContinue = false;
                        break;
                    }
                }
                fetcher.close(); // 关闭上一个文件
                parserFinish(current.getName());
                if (needContinue) {// 读取下一个
                    File nextFile;
                    if (needWait) {
                        nextFile = binlogs.waitForNextFile(current);
                    } else {
                        nextFile = binlogs.getNextFile(current);
                    }
                    if (nextFile == null) {
                        break;
                    }
                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(nextFile.getName()));
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        }
    }

    public void dump(long timestampMills, SinkFunction func) throws IOException {
        List<File> currentBinlogs = binlogs.currentBinlogs();
        File current = currentBinlogs.get(currentBinlogs.size() - 1);
        long timestampSeconds = timestampMills / 1000;

        String binlogFilename = null;
        long binlogFileOffset = 0;

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current);
            context.setLogPosition(new LogPosition(current.getName()));
            while (running) {
                boolean needContinue = true;
                String lastXidLogFilename = current.getName();
                long lastXidLogFileOffset = 0;

                binlogFilename = lastXidLogFilename;
                binlogFileOffset = lastXidLogFileOffset;
                while (fetcher.fetch()) {
                    LogEvent event = decoder.decode(fetcher, context);
                    if (event != null) {
                        if (serverId != 0 && event.getServerId() != serverId) {
                            throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                        }

                        if (event.getWhen() > timestampSeconds) {
                            break;
                        }

                        needContinue = false;
                        if (LogEvent.QUERY_EVENT == event.getHeader().getType()) {
                            if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN")) {
                                binlogFilename = lastXidLogFilename;
                                binlogFileOffset = lastXidLogFileOffset;
                            } else if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "COMMIT")) {
                                lastXidLogFilename = current.getName();
                                lastXidLogFileOffset = event.getLogPos();
                            }
                        } else if (LogEvent.XID_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        } else if (LogEvent.FORMAT_DESCRIPTION_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        }
                    }
                }

                if (needContinue) {// 读取下一个
                    fetcher.close(); // 关闭上一个文件

                    File nextFile = binlogs.getBefore(current);
                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(current.getName()));
                } else {
                    break;// 跳出
                }
            }
        } finally {
            fetcher.close();
        }

        dump(binlogFilename, binlogFileOffset, func);
    }

    @Override
    public void dump(GtIdSet gtidSet, SinkFunction func) {
        throw new NotImplementedException(this.getClass().getName());
    }

    @Override
    public void dump(String binlogFileName, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
        File current = new File(directory, binlogFileName);
        if (!current.exists()) {
            throw new CanalParseException("binlog:" + binlogFileName + " is not found");
        }
        try (FileLogFetcher fetcher = new FileLogFetcher(bufferSize)) {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogFileName, binlogPosition));
            while (running) {
                boolean needContinue = true;
                LogEvent event;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }
                    if (serverId != 0 && event.getServerId() != serverId) {
                        throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                    }
                    if (!coprocessor.publish(event)) {
                        needContinue = false;
                        break;
                    }
                }
                fetcher.close(); // 关闭上一个文件
                parserFinish(binlogFileName);
                if (needContinue) {// 读取下一个
                    File nextFile;
                    if (needWait) {
                        nextFile = binlogs.waitForNextFile(current);
                    } else {
                        nextFile = binlogs.getNextFile(current);
                    }
                    if (nextFile == null) {
                        break;
                    }
                    current = nextFile;
                    fetcher.open(current);
                    binlogFileName = nextFile.getName();
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        }
    }

    private void parserFinish(String fileName) {
        if (parserListener != null) {
            parserListener.onFinish(fileName);
        }
    }

    @Override
    public void dump(long timestampMills, MultiStageCoprocessor coprocessor) throws IOException {
        List<File> currentBinlogs = binlogs.currentBinlogs();
        File current = currentBinlogs.get(currentBinlogs.size() - 1);
        long timestampSeconds = timestampMills / 1000;

        String binlogFilename = null;
        long binlogFileOffset = 0;

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current);
            context.setLogPosition(new LogPosition(current.getName()));
            while (running) {
                boolean needContinue = true;
                String lastXidLogFilename = current.getName();
                long lastXidLogFileOffset = 0;

                binlogFilename = lastXidLogFilename;
                binlogFileOffset = lastXidLogFileOffset;
                while (fetcher.fetch()) {
                    LogEvent event = decoder.decode(fetcher, context);
                    if (event != null) {
                        if (serverId != 0 && event.getServerId() != serverId) {
                            throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                        }

                        if (event.getWhen() > timestampSeconds) {
                            break;
                        }

                        needContinue = false;
                        if (LogEvent.QUERY_EVENT == event.getHeader().getType()) {
                            if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN")) {
                                binlogFilename = lastXidLogFilename;
                                binlogFileOffset = lastXidLogFileOffset;
                            } else if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "COMMIT")) {
                                lastXidLogFilename = current.getName();
                                lastXidLogFileOffset = event.getLogPos();
                            }
                        } else if (LogEvent.XID_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        } else if (LogEvent.FORMAT_DESCRIPTION_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        }
                    }
                }

                if (needContinue) {// 读取下一个
                    fetcher.close(); // 关闭上一个文件

                    File nextFile = binlogs.getBefore(current);
                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(current.getName()));
                } else {
                    break;// 跳出
                }
            }
        } finally {
            fetcher.close();
        }
        dump(binlogFilename, binlogFileOffset, coprocessor);
    }

    @Override
    public void dump(GtIdSet gtidSet, MultiStageCoprocessor coprocessor) {
        throw new NotImplementedException(this.getClass().getName());
    }

    public ErosaConnection fork() {
        LocalBinLogConnection connection = new LocalBinLogConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        connection.setNeedWait(this.needWait);
        return connection;
    }

    @Override
    public long queryServerId() {
        return 0;
    }

    public boolean isNeedWait() {
        return needWait;
    }

    public void setNeedWait(boolean needWait) {
        this.needWait = needWait;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public void setParserListener(FileParserListener parserListener) {
        this.parserListener = parserListener;
    }

    public interface FileParserListener {
        void onFinish(String fileName);
    }
}
