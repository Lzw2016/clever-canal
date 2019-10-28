package org.clever.canal.parse.dbsync.binlog.event;

import org.clever.canal.parse.dbsync.binlog.LogBuffer;
import org.clever.canal.parse.dbsync.binlog.LogEvent;

/**
 * Delete_file_log_event.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class DeleteFileLogEvent extends LogEvent {

    private final long fileId;

    /* DF = "Delete File" */
    public static final int DF_FILE_ID_OFFSET = 0;

    public DeleteFileLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        buffer.position(commonHeaderLen + DF_FILE_ID_OFFSET);
        fileId = buffer.getUint32(); // DF_FILE_ID_OFFSET
    }

    public final long getFileId() {
        return fileId;
    }
}
