package org.clever.canal.parse.dbsync.binlog.event.mariadb;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.LogEvent;
import org.clever.canal.parse.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.LogHeader;

/**
 * mariadb的Start_encryption_log_event
 */
@SuppressWarnings("unused")
public class StartEncryptionLogEvent extends LogEvent {

    public StartEncryptionLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);
    }
}
