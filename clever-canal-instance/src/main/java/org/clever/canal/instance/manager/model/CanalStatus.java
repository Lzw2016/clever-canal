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

    public boolean isStart() {
        return this.equals(CanalStatus.START);
    }

    public boolean isStop() {
        return this.equals(CanalStatus.STOP);
    }
}
