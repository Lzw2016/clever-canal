package org.clever.canal.parse.exception;

import org.clever.canal.common.CanalException;

/**
 * canal 异常定义
 */
public class CanalParseException extends CanalException {
    private static final long serialVersionUID = -7288830284122672209L;

    public CanalParseException(String errorCode) {
        super(errorCode);
    }

    public CanalParseException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalParseException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalParseException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalParseException(Throwable cause) {
        super(cause);
    }

}
