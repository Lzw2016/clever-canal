package org.clever.canal.parse.dbsync.binlog;

import org.clever.canal.parse.dbsync.binlog.event.*;
import org.clever.canal.parse.dbsync.binlog.event.TableMapLogEvent.ColumnInfo;
import org.clever.canal.parse.dbsync.binlog.event.mariadb.AnnotateRowsEvent;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

@SuppressWarnings({"WeakerAccess", "unused", "StatementWithEmptyBody"})
public class BaseLogFetcherTest {

    protected String binlogFileName = "mysql-bin.000001";
    protected Charset charset = StandardCharsets.UTF_8;

    protected void parseQueryEvent(QueryLogEvent event) {
        System.out.println(String.format("================> binlog[%s:%s] , name[%s]",
                binlogFileName,
                event.getHeader().getLogPos() - event.getHeader().getEventLen(),
                event.getCatalog()));
        System.out.println("sql : " + event.getQuery());
    }

    protected void parseRowsQueryEvent(RowsQueryLogEvent event) throws Exception {
        System.out.println(String.format("================> binlog[%s:%s]", binlogFileName, event.getHeader()
                .getLogPos() - event.getHeader().getEventLen()));
        System.out.println("sql : " + new String(event.getRowsQuery().getBytes(StandardCharsets.ISO_8859_1), charset.name()));
    }

    protected void parseAnnotateRowsEvent(AnnotateRowsEvent event) throws Exception {
        System.out.println(String.format("================> binlog[%s:%s]", binlogFileName, event.getHeader()
                .getLogPos() - event.getHeader().getEventLen()));
        System.out.println("sql : " + new String(event.getRowsQuery().getBytes(StandardCharsets.ISO_8859_1), charset.name()));
    }

    protected void parseXidEvent(XidLogEvent event) {
        System.out.println(String.format("================> binlog[%s:%s]", binlogFileName, event.getHeader()
                .getLogPos() - event.getHeader().getEventLen()));
        System.out.println("xid : " + event.getXid());
    }

    protected void parseRowsEvent(RowsLogEvent event) {
        try {
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s]",
                    binlogFileName,
                    event.getHeader().getLogPos() - event.getHeader().getEventLen(),
                    event.getTable().getDbName(),
                    event.getTable().getTableName()));
            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();
            while (buffer.nextOneRow(columns)) {
                // 处理row记录
                int type = event.getHeader().getType();
                if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                    // insert的记录放在before字段中
                    parseOneRow(event, buffer, columns, true);
                } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                    // delete的记录放在before字段中
                    parseOneRow(event, buffer, columns, false);
                } else {
                    // update需要处理before/after
                    System.out.println("-------> before");
                    parseOneRow(event, buffer, columns, false);
                    if (!buffer.nextOneRow(changeColumns, true)) {
                        break;
                    }
                    System.out.println("-------> after");
                    parseOneRow(event, buffer, changeColumns, true);
                }

            }
        } catch (Exception e) {
            throw new RuntimeException("parse row data failed.", e);
        }
    }

    protected void parseOneRow(RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter) {
        TableMapLogEvent map = event.getTable();
        if (map == null) {
            throw new RuntimeException("not found TableMap with tid=" + event.getTableId());
        }

        final int columnCnt = map.getColumnCnt();
        final ColumnInfo[] columnInfo = map.getColumnInfo();

        for (int i = 0; i < columnCnt; i++) {
            if (!cols.get(i)) {
                continue;
            }

            ColumnInfo info = columnInfo[i];
            buffer.nextValue(null, i, info.type, info.meta);

            if (buffer.isNull()) {
                //
            } else {
                final Serializable value = buffer.getValue();
                if (value instanceof byte[]) {
                    System.out.println(new String((byte[]) value));
                } else {
                    System.out.println(value);
                }
            }
        }
    }
}