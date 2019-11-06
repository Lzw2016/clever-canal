package org.clever.canal.prometheus.impl;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.clever.canal.protocol.CanalPacket.PacketType;
import org.clever.canal.server.netty.ClientInstanceProfiler;
import org.clever.canal.server.netty.model.ClientRequestResult;

import static org.clever.canal.prometheus.CanalInstanceExports.DEST;
import static org.clever.canal.prometheus.CanalInstanceExports.DEST_LABELS;

public class PrometheusClientInstanceProfiler implements ClientInstanceProfiler {
    /**
     * 单例
     */
    public static final PrometheusClientInstanceProfiler Instance = new PrometheusClientInstanceProfiler();

    private static final long NANO_PER_MILLI = 1000 * 1000L;
    private static final String PACKET_TYPE = "canal_instance_client_packets";
    private static final String OUTBOUND_BYTES = "canal_instance_client_bytes";
    private static final String EMPTY_BATCHES = "canal_instance_client_empty_batches";
    private static final String ERRORS = "canal_instance_client_request_error";
    private static final String LATENCY = "canal_instance_client_request_latency";

    private final Counter outboundCounter;
    private final Counter packetsCounter;
    private final Counter emptyBatchesCounter;
    private final Counter errorsCounter;
    private final Histogram responseLatency;
    private volatile boolean running = false;

    private PrometheusClientInstanceProfiler() {
        this.outboundCounter = Counter.build()
                .labelNames(DEST_LABELS)
                .name(OUTBOUND_BYTES)
                .help("Total bytes sent to client.")
                .create();
        this.packetsCounter = Counter.build()
                .labelNames(new String[]{DEST, "packetType"})
                .name(PACKET_TYPE)
                .help("Total packets sent to client.")
                .create();
        this.emptyBatchesCounter = Counter.build()
                .labelNames(DEST_LABELS)
                .name(EMPTY_BATCHES)
                .help("Total empty batches sent to client.")
                .create();
        this.errorsCounter = Counter.build()
                .labelNames(new String[]{DEST, "errorCode"})
                .name(ERRORS)
                .help("Total client request errors.")
                .create();
        this.responseLatency = Histogram.build()
                .labelNames(DEST_LABELS)
                .name(LATENCY)
                .help("Client request latency.")
                // buckets in milliseconds
                .buckets(2.5, 10.0, 25.0, 100.0)
                .create();
    }

    @Override
    public void profiling(ClientRequestResult result) {
        String destination = result.getDestination();
        PacketType type = result.getType();
        outboundCounter.labels(destination).inc(result.getAmount());
        short errorCode = result.getErrorCode();
        if (errorCode > 0) {
            errorsCounter.labels(destination, Short.toString(errorCode)).inc();
        }
        long latency = result.getLatency();
        responseLatency.labels(destination).observe(((double) latency) / NANO_PER_MILLI);
        if (type == PacketType.GET) {
            boolean empty = result.isEmpty();
            // 区分一下空包
            if (empty) {
                emptyBatchesCounter.labels(destination).inc();
            } else {
                packetsCounter.labels(destination, type.name()).inc();
            }
            // reserve for others
        } else {
            packetsCounter.labels(destination, type.name()).inc();
        }
    }

    @Override
    public void start() {
        if (outboundCounter != null) {
            outboundCounter.register();
        }
        if (packetsCounter != null) {
            packetsCounter.register();
        }
        if (emptyBatchesCounter != null) {
            emptyBatchesCounter.register();
        }
        if (errorsCounter != null) {
            errorsCounter.register();
        }
        if (responseLatency != null) {
            responseLatency.register();
        }
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        if (outboundCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(outboundCounter);
        }
        if (packetsCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(packetsCounter);
        }
        if (emptyBatchesCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(emptyBatchesCounter);
        }
        if (errorsCounter != null) {
            CollectorRegistry.defaultRegistry.unregister(errorsCounter);
        }
        if (responseLatency != null) {
            CollectorRegistry.defaultRegistry.unregister(responseLatency);
        }
    }

    @Override
    public boolean isStart() {
        return running;
    }
}
