package org.clever.canal.sink.exception;

import org.clever.canal.common.CanalException;

/**
 * canal 异常定义
 */
@SuppressWarnings("unused")
public class CanalSinkException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalSinkException(String errorCode) {
        super(errorCode);
    }

    public CanalSinkException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalSinkException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalSinkException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalSinkException(Throwable cause) {
        super(cause);
    }
}
