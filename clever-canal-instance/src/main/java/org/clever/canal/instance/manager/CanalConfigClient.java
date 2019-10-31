package org.clever.canal.instance.manager;

import org.clever.canal.instance.manager.model.Canal;

/**
 * 对应canal的配置
 */
@SuppressWarnings({"unused"})
public class CanalConfigClient {

    /**
     * 根据对应的 destination 查询Canal信息
     */
    public Canal findCanal(String destination) {
        // TODO 根据自己的业务实现
        throw new UnsupportedOperationException();
    }

    /**
     * 根据对应的 destination 查询filter信息
     */
    public String findFilter(String destination) {
        // TODO 根据自己的业务实现
        throw new UnsupportedOperationException();
    }
}
