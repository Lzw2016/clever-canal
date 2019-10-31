package org.clever.canal.meta.exception;

import org.clever.canal.common.CanalException;

@SuppressWarnings("unused")
public class CanalMetaManagerException extends CanalException {

    private static final long serialVersionUID = -654893533794556357L;

    public CanalMetaManagerException(String errorCode) {
        super(errorCode);
    }

    public CanalMetaManagerException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalMetaManagerException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalMetaManagerException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalMetaManagerException(Throwable cause) {
        super(cause);
    }
}
