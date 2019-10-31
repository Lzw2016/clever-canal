package org.clever.canal.server;


import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.server.exception.CanalServerException;

/**
 * 对应canal整个服务实例，一个jvm实例只有一份server
 */
public interface CanalServer extends CanalLifeCycle {

    /**
     * 启动
     */
    void start() throws CanalServerException;

    /**
     * 停止
     */
    void stop() throws CanalServerException;
}
