package org.clever.canal.server.netty;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.server.netty.model.ClientRequestResult;


public class CanalServerWithNettyProfiler {

    public static final ClientInstanceProfiler NOP = new DefaultClientInstanceProfiler();
    private ClientInstanceProfiler instanceProfiler;

    private static class SingletonHolder {
        private static CanalServerWithNettyProfiler SINGLETON = new CanalServerWithNettyProfiler();
    }

    private CanalServerWithNettyProfiler() {
        this.instanceProfiler = NOP;
    }

    public static CanalServerWithNettyProfiler profiler() {
        return SingletonHolder.SINGLETON;
    }

    public void profiling(ClientRequestResult result) {
        instanceProfiler.profiling(result);
    }

    public void setInstanceProfiler(ClientInstanceProfiler instanceProfiler) {
        this.instanceProfiler = instanceProfiler;
    }

    private static class DefaultClientInstanceProfiler extends AbstractCanalLifeCycle implements ClientInstanceProfiler {
        @Override
        public void profiling(ClientRequestResult result) {
        }
    }
}
