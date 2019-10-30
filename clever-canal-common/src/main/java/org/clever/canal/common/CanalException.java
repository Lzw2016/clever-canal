package org.clever.canal.common;

public class CanalException extends RuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public CanalException(String errorCode) {
        super(errorCode);
    }

    public CanalException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalException(Throwable cause) {
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }
}
