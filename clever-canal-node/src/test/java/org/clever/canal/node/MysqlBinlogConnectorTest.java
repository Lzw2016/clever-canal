package org.clever.canal.node;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/30 10:53 <br/>
 */
@Slf4j
public class MysqlBinlogConnectorTest {

    @Test
    public void t1() throws IOException, InterruptedException, TimeoutException {
        BinaryLogClient client = new BinaryLogClient("127.0.0.1", 3306, "canal", "canal");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {

            @Override
            public void onConnect(BinaryLogClient client) {
                log.info("onConnect");
                // client.setBinlogFilename();
                // client.setBinlogPosition();
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                log.info("onCommunicationFailure");
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                log.info("onEventDeserializationFailure");
            }

            @Override
            public void onDisconnect(BinaryLogClient client) {
                log.info("onDisconnect");
            }
        });
        client.registerEventListener(event -> log.info("### Header {} | Data {}", event.getHeader().toString(), event.getData().toString()));
        client.connect(5000L);
        log.info("### {}", "123");
        Thread.sleep(1000 * 10);
        log.info("### end");
        client.disconnect();
    }
}
