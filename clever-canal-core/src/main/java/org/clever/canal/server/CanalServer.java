package org.clever.canal.server;


import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.server.exception.CanalServerException;

/**
 * 对应canal整个服务实例，一个jvm实例只有一份server
 */
@SuppressWarnings("unused")
public interface CanalServer extends CanalLifeCycle {

    void start() throws CanalServerException;

    void stop() throws CanalServerException;
}
