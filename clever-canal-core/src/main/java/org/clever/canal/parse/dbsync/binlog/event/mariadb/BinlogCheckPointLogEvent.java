package org.clever.canal.parse.dbsync.binlog.event.mariadb;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.IgnorableLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.LogHeader;

/**
 * mariadb10的BINLOG_CHECKPOINT_EVENT类型
 */
@SuppressWarnings("unused")
public class BinlogCheckPointLogEvent extends IgnorableLogEvent {

    public BinlogCheckPointLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
        // do nothing , just mariadb binlog checkpoint
    }
}
