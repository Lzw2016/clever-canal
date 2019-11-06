package org.clever.canal.prometheus;

import org.clever.canal.spi.CanalMetricsProvider;
import org.clever.canal.spi.CanalMetricsService;

public class PrometheusProvider implements CanalMetricsProvider {
    @Override
    public CanalMetricsService getService() {
        return PrometheusService.Instance;
    }
}
