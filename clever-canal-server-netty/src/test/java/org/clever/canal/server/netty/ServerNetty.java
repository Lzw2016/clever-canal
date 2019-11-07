package org.clever.canal.server.netty;

import lombok.extern.slf4j.Slf4j;
import org.clever.canal.instance.manager.CanalConfigClient;
import org.clever.canal.instance.manager.ManagerCanalInstanceGenerator;
import org.clever.canal.instance.manager.model.*;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.clever.canal.store.model.BatchMode;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 11:42 <br/>
 */
@Slf4j
public class ServerNetty {
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

        // 配置 CanalServerWithEmbedded
        ManagerCanalInstanceGenerator managerCanalInstanceGenerator = new ManagerCanalInstanceGenerator(canalConfigClient);
        CanalServerWithEmbedded canalServerWithEmbedded = CanalServerWithEmbedded.Instance;
        canalServerWithEmbedded.setCanalInstanceGenerator(managerCanalInstanceGenerator);
        // 启动 CanalServerWithNetty
        CanalServerWithNetty.Instance.start();
        canalServerWithEmbedded.start("test");
        log.info("### started");
        Thread.sleep(1000 * 3);
        log.info("### end");
        // canalServerWithEmbedded.stop();
        CanalServerWithNetty.Instance.stop();
    }
}
