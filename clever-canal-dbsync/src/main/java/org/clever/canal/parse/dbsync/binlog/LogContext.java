package org.clever.canal.parse.dbsync.binlog;

import org.clever.canal.parse.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.GtidLogEvent;
import org.clever.canal.parse.dbsync.binlog.event.TableMapLogEvent;
import org.clever.canal.parse.driver.mysql.packets.GTIDSet;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Document Me!! NOTE: Log context will NOT write multi-threaded.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class LogContext {

    private final Map<Long, TableMapLogEvent> mapOfTable = new HashMap<>();

    private FormatDescriptionLogEvent formatDescription;

    private LogPosition logPosition;

    private GTIDSet gtidSet;

    private GtidLogEvent gtidLogEvent; // save current gtid log event

    public LogContext() {
        this.formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
    }

    public LogContext(FormatDescriptionLogEvent descriptionEvent) {
        this.formatDescription = descriptionEvent;
    }

    public final LogPosition getLogPosition() {
        return logPosition;
    }

    public final void setLogPosition(LogPosition logPosition) {
        this.logPosition = logPosition;
    }

    public final FormatDescriptionLogEvent getFormatDescription() {
        return formatDescription;
    }

    public final void setFormatDescription(FormatDescriptionLogEvent formatDescription) {
        this.formatDescription = formatDescription;
    }

    public final void putTable(TableMapLogEvent mapEvent) {
        mapOfTable.put(mapEvent.getTableId(), mapEvent);
    }

    public final TableMapLogEvent getTable(final long tableId) {
        return mapOfTable.get(tableId);
    }

    public final void clearAllTables() {
        mapOfTable.clear();
    }

    public void reset() {
        formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
        mapOfTable.clear();
    }

    public GTIDSet getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(GTIDSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    public GtidLogEvent getGtidLogEvent() {
        return gtidLogEvent;
    }

    public void setGtidLogEvent(GtidLogEvent gtidLogEvent) {
        this.gtidLogEvent = gtidLogEvent;
    }
}
