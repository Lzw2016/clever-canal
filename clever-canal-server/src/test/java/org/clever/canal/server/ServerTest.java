package org.clever.canal.server;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.clever.canal.instance.manager.CanalConfigClient;
import org.clever.canal.instance.manager.ManagerCanalInstanceGenerator;
import org.clever.canal.instance.manager.model.*;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.Message;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.clever.canal.store.model.BatchMode;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:26 <br/>
 */
@Slf4j
public class ServerTest {

    private AtomicLong sum = new AtomicLong(0);
    private long start = 0;

    @Test
    public void t1() throws InterruptedException {
        CanalConfigClient canalConfigClient = new CanalConfigClient() {
            @Override
            public Canal findCanal(String destination) {
                CanalParameter canalParameter = new CanalParameter();
                // 配置 CanalMetaManager
                canalParameter.setMetaMode(MetaMode.LOCAL_FILE);
                // 配置 CanalEventStore
                canalParameter.setStorageMode(StorageMode.MEMORY);
                canalParameter.setStorageBatchMode(BatchMode.ITEM_SIZE);
                canalParameter.setMemoryStorageRawEntry(false);
                canalParameter.setDdlIsolation(true);
                // 配置 CanalEventParser
                canalParameter.setBlackFilter(null);
                canalParameter.setSourcingType(SourcingType.MYSQL);
                canalParameter.setGtIdEnable(true);
                canalParameter.setSlaveId(123L);
                canalParameter.addGroupDbAddresses(new DataSourcing(SourcingType.MYSQL, new InetSocketAddress("192.168.31.40", 3306)));
                // canalParameter.addGroupDbAddresses(new DataSourcing(SourcingType.MYSQL, new InetSocketAddress("127.0.0.1", 3306)));
                canalParameter.setDbUsername("canal");
                canalParameter.setDbPassword("canal");
                // TsDb
                canalParameter.setTsDbEnable(false);
                canalParameter.setTsDbJdbcUrl("jdbc:mysql://mysql.msvc.top:3306/clever-canal");
                canalParameter.setTsDbJdbcUserName("clever-canal");
                canalParameter.setTsDbJdbcPassword("lizhiwei");
                // 心跳检查信息
                canalParameter.setDetectingEnable(true);
                // 配置 CanalHAController
                canalParameter.setHaMode(HAMode.HEARTBEAT);
                canalParameter.setHeartbeatHaEnable(true);
                // 配置 CanalLogPositionManager
                canalParameter.setLogPositionMode(LogPositionMode.META);
                return new Canal(1L, "test", canalParameter);
            }

            @Override
            public String findFilter(String destination) {
                return ".*";
            }
        };

        // 启动服务端
        ManagerCanalInstanceGenerator managerCanalInstanceGenerator = new ManagerCanalInstanceGenerator(canalConfigClient);
        CanalServerWithEmbedded canalServerWithEmbedded = CanalServerWithEmbedded.Instance;
        canalServerWithEmbedded.setCanalInstanceGenerator(managerCanalInstanceGenerator);
        canalServerWithEmbedded.start();
        canalServerWithEmbedded.start("test");

        // 启动客户端
        ClientIdentity clientIdentity = new ClientIdentity();
        clientIdentity.setClientId((short) 12);
        clientIdentity.setDestination("test");
        clientIdentity.setFilter("test\\.demo_store");

        // 订阅
        canalServerWithEmbedded.subscribe(clientIdentity);
        final int batchSize = 100;
        // 消费线程
        Thread thread = new Thread(() -> {
            for (int i = 0; i < 10000000; i++) {
                if (start <= 0) {
                    log.warn("### 开始！");
                }
                Message message = canalServerWithEmbedded.get(clientIdentity, batchSize, 10L, TimeUnit.DAYS);
                if (start <= 0) {
                    start = System.currentTimeMillis();
                }
                if (message.isRaw()) {
                    message.getRawEntries().forEach(rawEntry -> {
                        try {
                            printf(CanalEntry.Entry.parseFrom(rawEntry));
                        } catch (InvalidProtocolBufferException e) {
                            log.error("", e);
                        }
                    });
                } else {
                    message.getEntries().forEach(this::printf);
                }
                if ((i * batchSize) % 1000 == 0) {
                    log.warn("### 处理速度： {}(个/ms)", (sum.get() * 1.0) / (System.currentTimeMillis() - start));
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        Thread.sleep(1000 * 1000);
        log.info("### end");
        canalServerWithEmbedded.stop();
    }

    private void printf(CanalEntry.Entry entry) {
        if (entry == null) {
            log.info("### entry = null");
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(
                String.format(
                        "[Header]### entryType=[%s] logfileName=[%s] | logfileOffset=[%s] | executeTime=[%s] | schemaName=[%s] | tableName=[%s] | eventType=[%s]",
                        entry.getEntryType(),
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(),
                        entry.getHeader().getExecuteTime(),
                        entry.getHeader().getSchemaName(),
                        entry.getHeader().getTableName(),
                        entry.getHeader().getEventType()
                )
        ).append("\n");
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            sb.append("[RowChange]### ");
            if (rowChange.getIsDdl()) {
                sum.incrementAndGet();
                sb.append("Sql=[").append(rowChange.getSql()).append("]");
            } else {
                sb.append("\n");
                int count = 0;
                for (CanalEntry.RowData rowData : rowChange.getRowDataList()) {
                    count++;
                    sum.incrementAndGet();
                    sb.append("\t").append(count).append("[Before]# ");
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        sb.append(column.getName()).append("=").append(column.getValue()).append(" | ");
                    }
                    sb.append("\n");
                    sb.append("\t").append(count).append("[After ]# ");
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        sb.append(column.getName()).append("=").append(column.getValue()).append(" | ");
                    }
                }
            }
            sb.append("\n");
            sb.append("-------------------------\n");
        } catch (InvalidProtocolBufferException e) {
            log.error("", e);
        }
        log.info(sb.toString());
    }
}
/*
Column
index
sqlType     [jdbc type]
name        [column name]
isKey       [是否为主键]
updated     [是否发生过变更]
isNull      [值是否为null]
value       [具体的内容，注意为string文本]
*/