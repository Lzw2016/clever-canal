package org.clever.canal.store.model;

/**
 * 批处理模式
 */
public enum BatchMode {

    /**
     * 对象数量
     */
    ITEM_SIZE,

    /**
     * 内存大小
     */
    MEM_SIZE;

    public boolean isItemSize() {
        return this == BatchMode.ITEM_SIZE;
    }

    public boolean isMemSize() {
        return this == BatchMode.MEM_SIZE;
    }
}
