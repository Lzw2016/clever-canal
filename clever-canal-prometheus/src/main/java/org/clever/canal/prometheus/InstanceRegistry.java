package org.clever.canal.prometheus;

import org.clever.canal.instance.core.CanalInstance;

/**
 * CanalInstance 实例的注册接口
 */
public interface InstanceRegistry {
    /**
     * 注册监控一个 CanalInstance
     */
    void register(CanalInstance instance);

    /**
     * 取消注册监控一个 CanalInstance
     */
    void unregister(CanalInstance instance);
}
