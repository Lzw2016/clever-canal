package org.clever.canal.parse.inbound.mysql.rds;

import org.apache.commons.lang3.StringUtils;
import org.clever.canal.parse.exception.PositionNotFoundException;
import org.clever.canal.parse.inbound.ParserExceptionHandler;
import org.clever.canal.parse.inbound.mysql.MysqlEventParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * aliyun rds的binlog parser支持
 *
 * <pre>
 * 注意点：aliyun的binlog会有定期清理并备份到oss上, 这里实现了一份自动下载oss+rds binlog的机制
 * </pre>
 */
@SuppressWarnings({"unused"})
public class RdsBinlogEventParserProxy extends MysqlEventParser {
    private final Logger logger = LoggerFactory.getLogger(RdsBinlogEventParserProxy.class);
    
    private String rdsOpenApiUrl = "https://rds.aliyuncs.com/"; // openapi地址
    private String accesskey;                                   // 云账号的ak
    private String secretkey;                                   // 云账号sk
    private String instanceId;                                  // rds实例id
    private String directory;                                   // binlog目录
    private int batchFileSize = 4;                              // 最多下载的binlog文件数量

    private RdsLocalBinlogEventParser rdsLocalBinlogEventParser = null;
    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "rds-binlog-daemon-thread");
        t.setDaemon(true);
        return t;
    });

    @Override
    public void start() {
        if (rdsLocalBinlogEventParser == null && StringUtils.isNotEmpty(accesskey) && StringUtils.isNotEmpty(secretkey) && StringUtils.isNotEmpty(instanceId)) {
            rdsLocalBinlogEventParser = new RdsLocalBinlogEventParser();
            // rds oss mode
            setRdsOssMode(true);
            final ParserExceptionHandler targetHandler = this.getParserExceptionHandler();
            if (directory == null) {
                directory = System.getProperty("java.io.tmpdir", "/tmp") + "/" + destination;
            }
            rdsLocalBinlogEventParser.setLogPositionManager(this.getLogPositionManager());
            rdsLocalBinlogEventParser.setDestination(destination);
            rdsLocalBinlogEventParser.setAlarmHandler(this.getAlarmHandler());
            rdsLocalBinlogEventParser.setConnectionCharset(this.connectionCharset);
            rdsLocalBinlogEventParser.setConnectionCharsetNumber(this.connectionCharsetNumber);
            rdsLocalBinlogEventParser.setEnableTsDb(this.enableTsDb);
            rdsLocalBinlogEventParser.setEventBlackFilter(this.eventBlackFilter);
            rdsLocalBinlogEventParser.setFilterQueryDcl(this.filterQueryDcl);
            rdsLocalBinlogEventParser.setFilterQueryDdl(this.filterQueryDdl);
            rdsLocalBinlogEventParser.setFilterQueryDml(this.filterQueryDml);
            rdsLocalBinlogEventParser.setFilterRows(this.filterRows);
            rdsLocalBinlogEventParser.setFilterTableError(this.filterTableError);
            // rdsLocalBinlogEventParser.setIsGTIDMode(this.isGTIDMode);
            rdsLocalBinlogEventParser.setMasterInfo(this.masterInfo);
            rdsLocalBinlogEventParser.setEventFilter(this.eventFilter);
            rdsLocalBinlogEventParser.setMasterPosition(this.masterPosition);
            rdsLocalBinlogEventParser.setTransactionSize(this.transactionSize);
            rdsLocalBinlogEventParser.setUrl(this.rdsOpenApiUrl);
            rdsLocalBinlogEventParser.setAccessKey(this.accesskey);
            rdsLocalBinlogEventParser.setSecretKey(this.secretkey);
            rdsLocalBinlogEventParser.setInstanceId(this.instanceId);
            rdsLocalBinlogEventParser.setEventSink(eventSink);
            rdsLocalBinlogEventParser.setDirectory(directory);
            rdsLocalBinlogEventParser.setBatchFileSize(batchFileSize);
            rdsLocalBinlogEventParser.setParallel(this.parallel);
            rdsLocalBinlogEventParser.setParallelBufferSize(this.parallelBufferSize);
            rdsLocalBinlogEventParser.setParallelThreadSize(this.parallelThreadSize);
            rdsLocalBinlogEventParser.setFinishListener(() -> executorService.execute(() -> {
                rdsLocalBinlogEventParser.stop();
                RdsBinlogEventParserProxy.this.start();
            }));
            this.setParserExceptionHandler(e -> {
                handleMysqlParserException(e);
                if (targetHandler != null) {
                    targetHandler.handle(e);
                }
            });
        }
        super.start();
    }

    private void handleMysqlParserException(Throwable throwable) {
        if (throwable instanceof PositionNotFoundException) {
            logger.info("remove rds not found position, try download rds binlog!");
            executorService.execute(() -> {
                try {
                    logger.info("stop mysql parser!");
                    RdsBinlogEventParserProxy rdsBinlogEventParserProxy = RdsBinlogEventParserProxy.this;
                    long serverId = rdsBinlogEventParserProxy.getServerId();
                    rdsLocalBinlogEventParser.setServerId(serverId);
                    rdsBinlogEventParserProxy.stop();
                } catch (Throwable e) {
                    logger.info("handle exception failed", e);
                }

                try {
                    logger.info("start rds mysql binlog parser!");
                    rdsLocalBinlogEventParser.start();
                } catch (Throwable e) {
                    logger.info("handle exception failed", e);
                    rdsLocalBinlogEventParser.stop();
                    RdsBinlogEventParserProxy rdsBinlogEventParserProxy = RdsBinlogEventParserProxy.this;
                    rdsBinlogEventParserProxy.start();// 继续重试
                }
            });
        }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public boolean isStart() {
        return super.isStart();
    }

    public void setRdsOpenApiUrl(String rdsOpenApiUrl) {
        this.rdsOpenApiUrl = rdsOpenApiUrl;
    }

    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }

    public void setSecretkey(String secretkey) {
        this.secretkey = secretkey;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void setBatchFileSize(int batchFileSize) {
        this.batchFileSize = batchFileSize;
    }
}
