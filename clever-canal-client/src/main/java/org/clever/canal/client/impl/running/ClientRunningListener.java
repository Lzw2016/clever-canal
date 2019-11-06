package org.clever.canal.client.impl.running;

import java.net.InetSocketAddress;

/**
 * 触发一下main stem发生切换
 */
public interface ClientRunningListener {
    /**
     * 触发现在轮到自己做为active，需要载入上一个active的上下文数据
     */
    InetSocketAddress processActiveEnter();

    /**
     * 触发一下当前active模式失败
     */
    void processActiveExit();
}
