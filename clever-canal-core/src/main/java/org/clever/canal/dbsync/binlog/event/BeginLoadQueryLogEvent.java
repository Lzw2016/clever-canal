package org.clever.canal.dbsync.binlog.event;

import org.clever.canal.dbsync.binlog.LogBuffer;

/**
 * Event for the first block of file to be loaded, its only difference from
 * Append_block event is that this event creates or truncates existing file
 * before writing data.
 */
public final class BeginLoadQueryLogEvent extends AppendBlockLogEvent {

    public BeginLoadQueryLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
    }
}
