package org.clever.canal.instance.manager;

import org.clever.canal.instance.manager.model.Canal;

/**
 * 对应canal的配置
 */
public interface CanalConfigClient {
    /**
     * 根据对应的 destination 查询Canal信息
     */
    Canal findCanal(String destination);

    /**
     * 根据对应的 destination 查询filter信息
     */
    String findFilter(String destination);
}
