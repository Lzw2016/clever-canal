package org.clever.canal.server.netty;

import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.server.netty.listener.ChannelFutureAggregator.ClientRequestResult;

/**
 * @author Chuanyi Li
 */
public interface ClientInstanceProfiler extends CanalLifeCycle {

    void profiling(ClientRequestResult result);
}
