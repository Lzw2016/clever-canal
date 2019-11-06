package org.clever.canal.prometheus;

import org.clever.canal.spi.CanalMetricsProvider;
import org.clever.canal.spi.CanalMetricsService;

/**
 * @author Chuanyi Li
 */
public class PrometheusProvider implements CanalMetricsProvider {

    @Override
    public CanalMetricsService getService() {
        return PrometheusService.getInstance();
    }
}
