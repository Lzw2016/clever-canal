package org.clever.canal.instance.manager.model;

/**
 * 运行状态
 */
public enum CanalStatus {
    /**
     * 启动
     */
    START,
    /**
     * 停止
     */
    STOP;

    @SuppressWarnings("unused")
    public boolean isStart() {
        return this.equals(CanalStatus.START);
    }

    @SuppressWarnings("unused")
    public boolean isStop() {
        return this.equals(CanalStatus.STOP);
    }
}
