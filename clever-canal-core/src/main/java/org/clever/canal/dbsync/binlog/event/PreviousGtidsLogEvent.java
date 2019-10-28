package org.clever.canal.dbsync.binlog.event;

import org.clever.canal.dbsync.binlog.LogBuffer;
import org.clever.canal.dbsync.binlog.LogEvent;

/**
 * @since mysql 5.6
 */
@SuppressWarnings("unused")
public class PreviousGtidsLogEvent extends LogEvent {

    public PreviousGtidsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);
        // do nothing , just for mysql gtid search function
    }
}
