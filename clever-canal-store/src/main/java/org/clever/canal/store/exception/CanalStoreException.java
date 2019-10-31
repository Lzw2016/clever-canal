package org.clever.canal.store.exception;

import org.clever.canal.common.CanalException;

/**
 * canal 异常定义
 */
@SuppressWarnings("unused")
public class CanalStoreException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalStoreException(String errorCode) {
        super(errorCode);
    }

    public CanalStoreException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalStoreException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalStoreException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalStoreException(Throwable cause) {
        super(cause);
    }

}
