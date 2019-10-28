package org.clever.canal.parse.dbsync.binlog.event.mariadb;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.IgnorableLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.LogHeader;

/**
 * mariadb的GTID_LIST_EVENT类型
 */
public class MariaGtidListLogEvent extends IgnorableLogEvent {

    public MariaGtidListLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
        // do nothing , just ignore log event
    }
}
