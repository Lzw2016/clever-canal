package org.clever.canal.parse.dbsync.binlog.event;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.LogEvent;

/**
 * <pre>
 * Replication event to ensure to slave that master is alive.
 *   The event is originated by master's dump thread and sent straight to
 *   slave without being logged. Slave itself does not store it in relay log
 *   but rather uses a data for immediate checks and throws away the event.
 *
 *   Two members of the class log_ident and Log_event::log_pos comprise
 *   the event_coordinates instance. The coordinates that a heartbeat
 *   instance carries correspond to the last event master has sent from
 *   its binlog.
 * </pre>
 *
 * @since mysql 5.6
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class HeartbeatLogEvent extends LogEvent {

    public static final int FN_REFLEN = 512; /* Max length of full path-name */
    private int identLen;
    private String logIdent;

    public HeartbeatLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        identLen = buffer.limit() - commonHeaderLen;
        if (identLen > FN_REFLEN - 1) {
            identLen = FN_REFLEN - 1;
        }

        logIdent = buffer.getFullString(commonHeaderLen, identLen, LogBuffer.ISO_8859_1);
    }

    public int getIdentLen() {
        return identLen;
    }

    public String getLogIdent() {
        return logIdent;
    }
}
