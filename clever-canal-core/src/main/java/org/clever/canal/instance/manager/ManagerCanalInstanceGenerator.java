package org.clever.canal.instance.manager;

import org.clever.canal.instance.core.CanalInstance;
import org.clever.canal.instance.core.CanalInstanceGenerator;
import org.clever.canal.instance.manager.model.Canal;

/**
 * 基于manager生成对应的{@linkplain CanalInstance}
 */
@SuppressWarnings("unused")
public class ManagerCanalInstanceGenerator implements CanalInstanceGenerator {

    private CanalConfigClient canalConfigClient;

    public CanalInstance generate(String destination) {
        Canal canal = canalConfigClient.findCanal(destination);
        String filter = canalConfigClient.findFilter(destination);
        return new CanalInstanceWithManager(canal, filter);
    }

    // ================ setter / getter ================

    public void setCanalConfigClient(CanalConfigClient canalConfigClient) {
        this.canalConfigClient = canalConfigClient;
    }
}
