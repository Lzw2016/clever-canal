package org.clever.canal.dbsync.binlog.event.mariadb;

import org.clever.canal.dbsync.binlog.LogBuffer;
import org.clever.canal.dbsync.binlog.event.FormatDescriptionLogEvent;
import org.clever.canal.dbsync.binlog.event.IgnorableLogEvent;
import org.clever.canal.dbsync.binlog.event.LogHeader;

/**
 * mariadb的GTID_EVENT类型
 */
@SuppressWarnings("unused")
public class MariaGtidLogEvent extends IgnorableLogEvent {

    private long gtid;

    /**
     * <pre>
     * mariadb gtidlog event format
     *     uint<8> GTID sequence
     *     uint<4> Replication Domain ID
     *     uint<1> Flags
     *
     * 	if flag & FL_GROUP_COMMIT_ID
     * 	    uint<8> commit_id
     * 	else
     * 	    uint<6> 0
     * </pre>
     */

    public MariaGtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header, buffer, descriptionEvent);
        gtid = buffer.getUlong64().longValue();
        // do nothing , just ignore log event
    }

    public long getGtid() {
        return gtid;
    }
}
