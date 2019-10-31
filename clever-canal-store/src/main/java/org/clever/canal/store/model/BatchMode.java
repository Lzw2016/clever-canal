package org.clever.canal.store.model;

/**
 * 批处理模式
 */
@SuppressWarnings("unused")
public enum BatchMode {

    /**
     * 对象数量
     */
    ITEMSIZE,

    /**
     * 内存大小
     */
    MEMSIZE;

    public boolean isItemSize() {
        return this == BatchMode.ITEMSIZE;
    }

    public boolean isMemSize() {
        return this == BatchMode.MEMSIZE;
    }
}
