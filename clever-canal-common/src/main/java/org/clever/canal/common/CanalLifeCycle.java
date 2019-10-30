package org.clever.canal.common;

@SuppressWarnings("unused")
public interface CanalLifeCycle {

    /**
     * 启动
     */
    void start();

    /**
     * 停止
     */
    void stop();

    /**
     * 是否启动
     */
    boolean isStart();
}
