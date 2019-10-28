package org.clever.canal.common;

@SuppressWarnings("unused")
public interface CanalLifeCycle {

    void start();

    void stop();

    boolean isStart();
}
