package org.clever.canal.parse.dbsync.binlog.event.mariadb;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.IgnorableLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.LogHeader;

/**
 * mariadb的ANNOTATE_ROWS_EVENT类型
 */
@SuppressWarnings("unused")
public class AnnotateRowsEvent extends IgnorableLogEvent {

    private String rowsQuery;

    public AnnotateRowsEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
        final int commonHeaderLen = descriptionEvent.getCommonHeaderLen();
        final int postHeaderLen = descriptionEvent.getPostHeaderLen()[header.getType() - 1];

        int offset = commonHeaderLen + postHeaderLen;
        int len = buffer.limit() - offset;
        rowsQuery = buffer.getFullString(offset, len, LogBuffer.ISO_8859_1);
    }

    public String getRowsQuery() {
        return rowsQuery;
    }
}
