package org.clever.canal.common;

/**
 * 基本实现
 */
@SuppressWarnings({"unused"})
public abstract class AbstractCanalLifeCycle implements CanalLifeCycle {

    protected volatile boolean running = false; // 是否处于运行中

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new CanalException(this.getClass().getName() + " has startup , don't repeat start");
        }
        running = true;
    }

    public void stop() {
        if (!running) {
            throw new CanalException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;
    }
}
