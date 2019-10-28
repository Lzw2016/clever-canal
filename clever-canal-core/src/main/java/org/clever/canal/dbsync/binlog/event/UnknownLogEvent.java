package org.clever.canal.dbsync.binlog.event;

import org.clever.canal.dbsync.binlog.LogEvent;

/**
 * Unknown_log_event
 */
public final class UnknownLogEvent extends LogEvent {

    public UnknownLogEvent(LogHeader header) {
        super(header);
    }
}
