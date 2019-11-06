package org.clever.canal.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.clever.canal.instance.core.CanalInstance;
import org.clever.canal.prometheus.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public class CanalInstanceExports {
    private static final Logger logger = LoggerFactory.getLogger(CanalInstanceExports.class);
    public static final CanalInstanceExports Instance = new CanalInstanceExports();
    public static final String DEST = "destination";
    public static final String[] DEST_LABELS = {DEST};
    public static final List<String> DEST_LABELS_LIST = Collections.singletonList(DEST);

    private final Collector storeCollector;
    private final Collector entryCollector;
    private final Collector metaCollector;
    private final Collector sinkCollector;
    private final Collector parserCollector;

    private CanalInstanceExports() {
        this.storeCollector = StoreCollector.Instance;
        this.entryCollector = EntryCollector.Instance;
        this.metaCollector = MetaCollector.Instance;
        this.sinkCollector = SinkCollector.Instance;
        this.parserCollector = ParserCollector.Instance;
    }

    /**
     * 初始化监控
     */
    public void initialize() {
        storeCollector.register();
        entryCollector.register();
        metaCollector.register();
        sinkCollector.register();
        parserCollector.register();
    }

    /**
     * 停止监控
     */
    public void terminate() {
        CollectorRegistry.defaultRegistry.unregister(storeCollector);
        CollectorRegistry.defaultRegistry.unregister(entryCollector);
        CollectorRegistry.defaultRegistry.unregister(metaCollector);
        CollectorRegistry.defaultRegistry.unregister(sinkCollector);
        CollectorRegistry.defaultRegistry.unregister(parserCollector);
    }

    /**
     * 注册监控数据收集器
     */
    void register(CanalInstance instance) {
        requiredInstanceRegistry(storeCollector).register(instance);
        requiredInstanceRegistry(entryCollector).register(instance);
        requiredInstanceRegistry(metaCollector).register(instance);
        requiredInstanceRegistry(sinkCollector).register(instance);
        requiredInstanceRegistry(parserCollector).register(instance);
        logger.info("Successfully register metrics for instance {}.", instance.getDestination());
    }

    /**
     * 取消注册监控数据收集器
     */
    void unregister(CanalInstance instance) {
        requiredInstanceRegistry(storeCollector).unregister(instance);
        requiredInstanceRegistry(entryCollector).unregister(instance);
        requiredInstanceRegistry(metaCollector).unregister(instance);
        requiredInstanceRegistry(sinkCollector).unregister(instance);
        requiredInstanceRegistry(parserCollector).unregister(instance);
        logger.info("Successfully unregister metrics for instance {}.", instance.getDestination());
    }

    private InstanceRegistry requiredInstanceRegistry(Collector collector) {
        if (!(collector instanceof InstanceRegistry)) {
            throw new IllegalArgumentException("Canal prometheus collector need to implement InstanceRegistry.");
        }
        return (InstanceRegistry) collector;
    }
}
