package org.clever.canal.dbsync.binlog.event;

import org.clever.canal.dbsync.binlog.LogBuffer;
import org.clever.canal.dbsync.binlog.LogEvent;

/**
 * @since mysql 5.7
 */
@SuppressWarnings("unused")
public class ViewChangeEvent extends LogEvent {

    public ViewChangeEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);
    }
}
