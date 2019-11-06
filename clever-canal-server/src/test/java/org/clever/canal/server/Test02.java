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

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:26 <br/>
 */
@Slf4j
public class Test02 {

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
                canalParameter.addGroupDbAddresses(new DataSourcing(SourcingType.MYSQL, new InetSocketAddress("127.0.0.1", 3306)));
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

        // 消费线程
        Thread thread = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                Message message = canalServerWithEmbedded.get(clientIdentity, 1, 10L, TimeUnit.DAYS);
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
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            CanalEntry.EventType eventType = rowChange.getEventType();
            log.info("### eventType={} | Sql={}", eventType, rowChange.getSql());
            for (CanalEntry.RowData rowData : rowChange.getRowDataList()) {
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
/*
Entry
    Header
        logfileName [binlog文件名]
        logfileOffset [binlog position]
        executeTime [binlog里记录变更发生的时间戳,精确到秒]
        schemaName
        tableName
        eventType [insert/update/delete类型]
    entryType   [事务头BEGIN/事务尾END/数据ROWDATA]
    storeValue  [byte数据,可展开，对应的类型为RowChange]

RowChange

isDdl       [是否是ddl变更操作，比如create table/drop table]

sql         [具体的ddl sql]

rowDatas    [具体insert/update/delete的变更数据，可为多条，1个binlog event事件可对应多条变更，比如批处理]

beforeColumns [Column类型的数组，变更前的数据字段]

afterColumns [Column类型的数组，变更后的数据字段]

Column

index

sqlType     [jdbc type]

name        [column name]

isKey       [是否为主键]

updated     [是否发生过变更]

isNull      [值是否为null]

value       [具体的内容，注意为string文本]
*/