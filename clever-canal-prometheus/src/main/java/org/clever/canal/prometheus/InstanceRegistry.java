package org.clever.canal.prometheus;

import org.clever.canal.instance.core.CanalInstance;


public interface InstanceRegistry {

    void register(CanalInstance instance);

    void unregister(CanalInstance instance);
}
