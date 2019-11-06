package org.clever.canal.client;

import java.net.SocketAddress;

/**
 * 集群节点访问控制接口
 */
public interface CanalNodeAccessStrategy {

    SocketAddress currentNode();

    SocketAddress nextNode();
}
