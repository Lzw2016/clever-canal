package org.clever.canal.parse.dbsync.binlog.event;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.LogEvent;

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
