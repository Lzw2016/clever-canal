package org.clever.canal.prometheus.impl;

import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import org.clever.canal.instance.core.CanalInstance;
import org.clever.canal.prometheus.InstanceRegistry;
import org.clever.canal.sink.CanalEventSink;
import org.clever.canal.sink.entry.EntryEventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.clever.canal.prometheus.CanalInstanceExports.DEST_LABELS_LIST;

public class SinkCollector extends Collector implements InstanceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(SinkCollector.class);
    /**
     * 单例对象
     */
    public static final SinkCollector Instance = new SinkCollector();

    private static final long NANO_PER_MILLI = 1000 * 1000L;
    private static final String SINK_BLOCKING_TIME = "canal_instance_sink_blocking_time";
    private static final String SINK_BLOCK_TIME_HELP = "Total sink blocking time in milliseconds";

    private final ConcurrentMap<String, SinkMetricsHolder> instances = new ConcurrentHashMap<>();

    private SinkCollector() {
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        CounterMetricFamily blockingCounter = new CounterMetricFamily(SINK_BLOCKING_TIME,
                SINK_BLOCK_TIME_HELP, DEST_LABELS_LIST);
        for (SinkMetricsHolder smh : instances.values()) {
            blockingCounter.addMetric(smh.destLabelValues, (smh.eventsSinkBlockingTime.doubleValue() / NANO_PER_MILLI));
        }
        mfs.add(blockingCounter);
        return mfs;
    }

    @Override
    public void register(CanalInstance instance) {
        final String destination = instance.getDestination();
        SinkMetricsHolder holder = new SinkMetricsHolder();
        holder.destLabelValues = Collections.singletonList(destination);
        CanalEventSink sink = instance.getEventSink();
        if (!(sink instanceof EntryEventSink)) {
            throw new IllegalArgumentException("CanalEventSink must be EntryEventSink");
        }
        EntryEventSink entrySink = (EntryEventSink) sink;
        holder.eventsSinkBlockingTime = entrySink.getEventsSinkBlockingTime();
        Preconditions.checkNotNull(holder.eventsSinkBlockingTime);
        SinkMetricsHolder old = instances.put(destination, holder);
        if (old != null) {
            logger.warn("Remote stale SinkCollector for instance {}.", destination);
        }
    }

    @Override
    public void unregister(CanalInstance instance) {
        final String destination = instance.getDestination();
        instances.remove(destination);
    }

    private class SinkMetricsHolder {
        private AtomicLong eventsSinkBlockingTime;
        private List<String> destLabelValues;
    }
}
