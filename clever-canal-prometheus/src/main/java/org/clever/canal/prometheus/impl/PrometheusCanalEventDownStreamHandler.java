package org.clever.canal.prometheus.impl;

import lombok.Getter;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalEntry.EntryType;
import org.clever.canal.sink.AbstractCanalEventDownStreamHandler;
import org.clever.canal.store.model.Event;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Chuanyi Li
 */
public class PrometheusCanalEventDownStreamHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    @Getter
    private final AtomicLong latestExecuteTime = new AtomicLong(System.currentTimeMillis());
    @Getter
    private final AtomicLong transactionCounter = new AtomicLong(0L);

    @Override
    public List<Event> before(List<Event> events) {
        long localExecTime = 0L;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                EntryType type = e.getEntryType();
                if (type == null) continue;
                switch (type) {
                    case TRANSACTION_BEGIN:
                    case ROW_DATA: {
                        long exec = e.getExecuteTime();
                        if (exec > 0) localExecTime = exec;
                        break;
                    }
                    case TRANSACTION_END: {
                        long exec = e.getExecuteTime();
                        if (exec > 0) localExecTime = exec;
                        transactionCounter.incrementAndGet();
                        break;
                    }
                    case ENTRY_HEARTBEAT:
                        CanalEntry.EventType eventType = e.getEventType();
                        if (eventType == CanalEntry.EventType.M_HEARTBEAT) {
                            localExecTime = System.currentTimeMillis();
                        }
                        break;
                    default:
                        break;
                }
            }
            if (localExecTime > 0) {
                latestExecuteTime.lazySet(localExecTime);
            }
        }
        return events;
    }

    @Override
    public void start() {

        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }
}
