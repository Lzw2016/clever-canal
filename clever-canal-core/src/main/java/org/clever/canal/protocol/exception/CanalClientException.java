package org.clever.canal.protocol.exception;

@SuppressWarnings("unused")
public class CanalClientException extends RuntimeException {

    private static final long serialVersionUID = -7545341502620139031L;

    public CanalClientException(String errorCode) {
        super(errorCode);
    }

    public CanalClientException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalClientException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalClientException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalClientException(Throwable cause) {
        super(cause);
    }
}
