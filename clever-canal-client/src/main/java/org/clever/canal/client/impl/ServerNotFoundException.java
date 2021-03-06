package org.clever.canal.client.impl;

import org.clever.canal.protocol.exception.CanalClientException;

@SuppressWarnings("unused")
public class ServerNotFoundException extends CanalClientException {
    private static final long serialVersionUID = -3471518241911601774L;

    public ServerNotFoundException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode, errorDesc, cause);
    }

    public ServerNotFoundException(String errorCode, String errorDesc) {
        super(errorCode, errorDesc);
    }

    public ServerNotFoundException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public ServerNotFoundException(String errorCode) {
        super(errorCode);
    }

    public ServerNotFoundException(Throwable cause) {
        super(cause);
    }
}
