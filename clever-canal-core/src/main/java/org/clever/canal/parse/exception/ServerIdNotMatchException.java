package org.clever.canal.parse.exception;

import org.clever.canal.common.CanalException;

@SuppressWarnings("unused")
public class ServerIdNotMatchException extends CanalException {

    private static final long serialVersionUID = -6124989280379293953L;

    public ServerIdNotMatchException(String errorCode) {
        super(errorCode);
    }

    public ServerIdNotMatchException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public ServerIdNotMatchException(String errorCode, String errorDesc) {
        super(errorCode, errorDesc);
    }

    public ServerIdNotMatchException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode, errorDesc, cause);
    }

    public ServerIdNotMatchException(Throwable cause) {
        super(cause);
    }
}
