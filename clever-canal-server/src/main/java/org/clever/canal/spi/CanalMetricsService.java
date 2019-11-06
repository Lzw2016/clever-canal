package org.clever.canal.spi;

import org.clever.canal.instance.core.CanalInstance;

/**
 * Canal server/instance metrics for export.
 * <strong>
 * Designed to be created by service provider.
 * </strong>
 *
 * @see CanalMetricsProvider
 */
public interface CanalMetricsService {
    /**
     * 设置CanalMetricsService的端口号
     *
     * @param port 端口号
     */
    void setServerPort(int port);

    /**
     * Initialization on canal server startup.(初始化并启动 CanalMetricsService)
     */
    void initialize();

    /**
     * Clean-up at canal server stop phase.(停止 CanalMetricsService)
     */
    void terminate();

    /**
     * 判断 CanalMetricsService 是否正在运行
     *
     * @return {@code true} if the metrics service is running, otherwise {@code false}.
     */
    boolean isRunning();

    /**
     * Register instance level metrics for specified instance.(注册监控一个 CanalInstance)
     *
     * @param instance {@link CanalInstance}
     */
    void register(CanalInstance instance);

    /**
     * Unregister instance level metrics for specified instance.(取消注册监控一个 CanalInstance)
     *
     * @param instance {@link CanalInstance}
     */
    void unregister(CanalInstance instance);
}
