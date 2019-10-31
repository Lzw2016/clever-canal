package org.clever.canal.server.exception;

import org.clever.canal.common.CanalException;

/**
 * canal 异常定义
 */
@SuppressWarnings("unused")
public class CanalServerException extends CanalException {
    private static final long serialVersionUID = -7288830284122672209L;

    public CanalServerException(String errorCode) {
        super(errorCode);
    }

    public CanalServerException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalServerException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalServerException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalServerException(Throwable cause) {
        super(cause);
    }
}
