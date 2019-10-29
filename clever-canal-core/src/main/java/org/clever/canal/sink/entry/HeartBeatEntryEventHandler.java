package org.clever.canal.sink.entry;

import org.clever.canal.protocol.CanalEntry.EntryType;
import org.clever.canal.sink.AbstractCanalEventDownStreamHandler;
import org.clever.canal.store.model.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * 处理一下一下heartbeat数据
 */
public class HeartBeatEntryEventHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    public List<Event> before(List<Event> events) {
        boolean existHeartBeat = false;
        for (Event event : events) {
            if (event.getEntryType() == EntryType.HEARTBEAT) {
                existHeartBeat = true;
                break;
            }
        }
        if (!existHeartBeat) {
            return events;
        } else {
            // 目前heartbeat和其他事件是分离的，保险一点还是做一下检查处理
            List<Event> result = new ArrayList<>();
            for (Event event : events) {
                if (event.getEntryType() != EntryType.HEARTBEAT) {
                    result.add(event);
                }
            }
            return result;
        }
    }
}
