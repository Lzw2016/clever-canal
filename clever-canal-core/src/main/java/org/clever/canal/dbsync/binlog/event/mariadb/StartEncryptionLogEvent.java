package org.clever.canal.dbsync.binlog.event.mariadb;

import org.clever.canal.dbsync.binlog.LogBuffer;
import org.clever.canal.dbsync.binlog.LogEvent;
import org.clever.canal.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.dbsync.binlog.event.LogHeader;

/**
 * mariadb的Start_encryption_log_event
 */
@SuppressWarnings("unused")
public class StartEncryptionLogEvent extends LogEvent {

    public StartEncryptionLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);
    }
}
