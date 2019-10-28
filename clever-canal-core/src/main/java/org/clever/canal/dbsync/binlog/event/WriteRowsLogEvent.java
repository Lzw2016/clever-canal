package org.clever.canal.dbsync.binlog.event;

import org.clever.canal.dbsync.binlog.LogBuffer;

/**
 * Log row insertions and updates. The event contain several insert/update rows
 * for a table. Note that each event contains only rows for one table.
 */
public final class WriteRowsLogEvent extends RowsLogEvent {

    public WriteRowsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
    }
}
