package org.clever.canal.parse.dbsync.binlog.exception;

import org.clever.canal.common.CanalException;

@SuppressWarnings("unused")
public class TableIdNotFoundException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public TableIdNotFoundException(String errorCode) {
        super(errorCode);
    }

    public TableIdNotFoundException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public TableIdNotFoundException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public TableIdNotFoundException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public TableIdNotFoundException(Throwable cause) {
        super(cause);
    }
}
