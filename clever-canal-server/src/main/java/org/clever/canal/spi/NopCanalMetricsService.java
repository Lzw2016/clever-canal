package org.clever.canal.spi;

import org.clever.canal.instance.core.CanalInstance;

/**
 * CanalMetricsService 的空实现
 */
public class NopCanalMetricsService implements CanalMetricsService {
    public static final NopCanalMetricsService NOP = new NopCanalMetricsService();

    private NopCanalMetricsService() {
    }

    @Override
    public void initialize() {
    }

    @Override
    public void terminate() {
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public void register(CanalInstance instance) {
    }

    @Override
    public void unregister(CanalInstance instance) {
    }

    @Override
    public void setServerPort(int port) {
    }
}
