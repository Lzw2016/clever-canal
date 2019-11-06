package org.clever.canal.spi;

/**
 * Use java service provider mechanism to provide {@link CanalMetricsService}.
 * <pre>
 * Example:
 * {@code
 *     ServiceLoader<CanalMetricsProvider> providers = ServiceLoader.load(CanalMetricsProvider.class);
 *     List<CanalMetricsProvider> list = new ArrayList<CanalMetricsProvider>();
 *     for (CanalMetricsProvider provider : providers) {
 *         list.add(provider);
 *     }
 * }
 * </pre>
 */
public interface CanalMetricsProvider {
    /**
     * 返回一个 CanalMetricsService 实例
     *
     * @return Impl of {@link CanalMetricsService}
     */
    CanalMetricsService getService();
}
