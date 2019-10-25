package org.clever.canal.node;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.instance.manager.CanalConfigClient;
import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.ManagerCanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaSnapshotDAO;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/25 16:39 <br/>
 */
@Slf4j
public class CanalTest {

    @Test
    public void t0() throws InterruptedException {
        Canal canal = new Canal();
        canal.setId(1L);
        canal.setName("test");
        canal.setDesc("test");
        // canal.setStatus(CanalStatus.START);
        canal.setGmtCreate(new Date());
        canal.setGmtModified(new Date());
        CanalParameter canalParameter = new CanalParameter();
        canal.setCanalParameter(canalParameter);
        canalParameter.setCanalId(1L);
        canalParameter.setDbAddresses(new ArrayList<InetSocketAddress>() {{
            add(new InetSocketAddress("127.0.0.1", 3306));
        }});
        canalParameter.setDbUsername("canal");
        canalParameter.setDbPassword("canal");
        canalParameter.setSlaveId(123L);
        canalParameter.setIndexMode(CanalParameter.IndexMode.MEMORY);

        CanalInstanceWithManager canalInstanceWithManager = new CanalInstanceWithManager(canal, ".*");
        canalInstanceWithManager.start();

        log.info("### {}", "123");
        Thread.sleep(1000 * 1000);
        log.info("### end");

        canalInstanceWithManager.stop();
    }

    @Test
    public void t1() throws InterruptedException {
        MysqlEventParser mysqlEventParser = new MysqlEventParser();
//        EventTransactionBuffer.TransactionFlushCallback transactionFlushCallback = new
        log.info("### {}", "123");
        Thread.sleep(1000 * 1000);
        log.info("### end");
    }

    @Test
    public void t2() throws InterruptedException {
        CanalConfigClient canalConfigClient = new CanalConfigClient() {
            @Override
            public Canal findCanal(String destination) {
                Canal canal = new Canal();
                canal.setId(1L);
                canal.setName("test");
                canal.setDesc("test");
                // canal.setStatus(CanalStatus.START);
                canal.setGmtCreate(new Date());
                canal.setGmtModified(new Date());
                CanalParameter canalParameter = new CanalParameter();
                canal.setCanalParameter(canalParameter);
                canalParameter.setCanalId(1L);
                canalParameter.setDbAddresses(new ArrayList<InetSocketAddress>() {{
                    add(new InetSocketAddress("127.0.0.1", 3306));
                }});
                canalParameter.setDbUsername("canal");
                canalParameter.setDbPassword("canal");
                canalParameter.setSlaveId(123L);
                canalParameter.setIndexMode(CanalParameter.IndexMode.MEMORY);
                return canal;
            }

            @Override
            public String findFilter(String destination) {
                return ".*";
            }
        };

        ManagerCanalInstanceGenerator managerCanalInstanceGenerator = new ManagerCanalInstanceGenerator();
        managerCanalInstanceGenerator.setCanalConfigClient(canalConfigClient);
        CanalServerWithEmbedded canalServerWithEmbedded = CanalServerWithEmbedded.instance();
        canalServerWithEmbedded.setCanalInstanceGenerator(managerCanalInstanceGenerator);
        canalServerWithEmbedded.setMetricsPort(13306);
        canalServerWithEmbedded.start();
        canalServerWithEmbedded.start("test");

        ClientIdentity clientIdentity = new ClientIdentity();
        clientIdentity.setClientId((short) 12);
        clientIdentity.setDestination("test");

        canalServerWithEmbedded.subscribe(clientIdentity);

        Thread thread = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                try {
                    Thread.sleep(1000 * 2);
                } catch (InterruptedException ignored) {
                }
                Message message = canalServerWithEmbedded.getWithoutAck(clientIdentity, 1);
                if (message.getRawEntries() == null || message.getRawEntries().size() <= 0) {
                    continue;
                }
                for (ByteString rawEntry : message.getRawEntries()) {
                    try {
                        CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(rawEntry);
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        log.info("### eventType={} | Sql={}", eventType, rowChange.getSql());
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                log.info("### BeforeColumn | {}={}", column.getName(), column.getValue());
                            }
                            log.info("### -----------------------------------------------------------------------------------------");
                            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                                log.info("### AfterColumn | {}={}", column.getName(), column.getValue());
                            }
                        }
                        log.info("### =============================================================================================");
                    } catch (InvalidProtocolBufferException e) {
                        log.error("", e);
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        Thread.sleep(1000 * 1000);
        log.info("### end");
        canalServerWithEmbedded.stop();
    }


    @Test
    public void t3() throws IOException, InterruptedException {
        // 1.构造Mysql连接
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 3306);
        MysqlConnection connection = new MysqlConnection(address, "canal", "canal", (byte) 33, null);
        connection.setCharset(StandardCharsets.UTF_8);
        connection.setReceivedBinlogBytes(new AtomicLong(0L));
        connection.setSlaveId(123L);
        connection.connect();

//        // 2. 启动一个心跳线程
//        long lastEntryTime = 3;
//        long interval = 3;
//        Timer timer = new Timer("HeartBeatTimeTask", true);
//        TimerTask timerTask = new TimerTask() {
//            public void run() {
//                try {
//                    // 如果未出现异常，或者有第一条正常数据
//                    long now = System.currentTimeMillis();
//                    if (((now - lastEntryTime) / 1000) >= interval) {
//                        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
//                        headerBuilder.setExecuteTime(now);
//                        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
//                        entryBuilder.setHeader(headerBuilder.build());
//                        entryBuilder.setEntryType(CanalEntry.EntryType.HEARTBEAT);
//                        CanalEntry.Entry entry = entryBuilder.build();
//                        // 提交到sink中，目前不会提交到store中，会在sink中进行忽略
//                        // consumeTheEventAndProfilingIfNecessary(Arrays.asList(entry));
//                    }
//                } catch (Throwable ignored) {
//                }
//            }
//        };
//        timer.schedule(timerTask, interval * 1000L, interval * 1000L);

        // 缓存表结构
        MysqlConnection metaConnection = connection.fork();
        metaConnection.connect();
        DatabaseTableMeta tableMetaTSDB = new DatabaseTableMeta();
        tableMetaTSDB.setConnection(metaConnection);
        tableMetaTSDB.setFilter(null);
        tableMetaTSDB.setBlackFilter(null);
        tableMetaTSDB.init("test");
        TableMetaCache tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
        tableMetaTSDB.setMetaHistoryDAO(new MetaHistoryDAO());
        tableMetaTSDB.setMetaSnapshotDAO(new MetaSnapshotDAO());

        // 解析binlog
        LogEventConvert convert = new LogEventConvert();
        convert.setCharset(StandardCharsets.UTF_8);
        convert.setFilterQueryDcl(false);
        convert.setFilterQueryDml(false);
        convert.setFilterQueryDdl(false);
        convert.setFilterRows(false);
        convert.setFilterTableError(false);
        convert.setUseDruidDdlFilter(true);
        convert.setTableMetaCache(tableMetaCache);
        convert.setNameFilter(new AviaterRegexFilter(".*"));
        EntryPosition position = new EntryPosition("mysql-bin.000007", 1078L);
        position.setServerId(1L);
        position.setTimestamp(System.currentTimeMillis());
        connection.dump(position.getJournalName(), position.getPosition(), event -> {
            CanalEntry.Entry entry;
            try {
                entry = convert.parse((LogEvent) event, false);
            } catch (Throwable e) {
                log.error("", e);
                return true;
            }
            if (entry == null) {
                return true;
            }
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {

                return false;
            }
            CanalEntry.EventType eventType = rowChange.getEventType();
            log.info("### {} | eventType={} | Sql={}", entry.getEntryType(), eventType, rowChange.getSql());
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                    log.info("### BeforeColumn | {}={}", column.getName(), column.getValue());
                }
                log.info("### -----------------------------------------------------------------------------------------");
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    log.info("### AfterColumn | {}={}", column.getName(), column.getValue());
                }
            }
            log.info("### =============================================================================================");
            return true;
        });

        // Thread.sleep(1000 * 1000);
        log.info("### end");
        connection.disconnect();
    }
}
