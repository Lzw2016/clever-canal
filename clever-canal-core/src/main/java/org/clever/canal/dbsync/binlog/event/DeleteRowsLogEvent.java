package org.clever.canal.dbsync.binlog.event;

import org.clever.canal.dbsync.binlog.LogBuffer;

/**
 * Log row deletions. The event contain several delete rows for a table. Note
 * that each event contains only rows for one table.
 */
public final class DeleteRowsLogEvent extends RowsLogEvent {

    public DeleteRowsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
    }
}
