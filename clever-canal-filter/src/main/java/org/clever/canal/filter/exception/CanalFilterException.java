package org.clever.canal.filter.exception;

import org.clever.canal.common.CanalException;

/**
 * canal 异常定义
 */
@SuppressWarnings("unused")
public class CanalFilterException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalFilterException(String errorCode) {
        super(errorCode);
    }

    public CanalFilterException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalFilterException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalFilterException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalFilterException(Throwable cause) {
        super(cause);
    }
}
