package org.clever.canal.parse.dbsync.binlog.event;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.LogEvent;

/**
 * Stop_log_event. The Post-Header and Body for this event type are empty; it
 * only has the Common-Header.
 */
@SuppressWarnings("unused")
public final class StopLogEvent extends LogEvent {

    public StopLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent description_event) {
        super(header);
    }
}
