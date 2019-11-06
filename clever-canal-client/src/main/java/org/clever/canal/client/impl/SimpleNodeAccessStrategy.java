package org.clever.canal.client.impl;

import org.clever.canal.client.CanalNodeAccessStrategy;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * 简单版本的node访问实现
 */
public class SimpleNodeAccessStrategy implements CanalNodeAccessStrategy {

    private List<SocketAddress> nodes = new ArrayList<>();
    private int index = 0;

    public SimpleNodeAccessStrategy(List<? extends SocketAddress> nodes) {
        if (nodes == null || nodes.size() < 1) {
            throw new IllegalArgumentException("at least 1 node required.");
        }
        this.nodes.addAll(nodes);
    }

    public SocketAddress nextNode() {
        try {
            return nodes.get(index);
        } finally {
            index = (index + 1) % nodes.size();
        }
    }

    @Override
    public SocketAddress currentNode() {
        return nodes.get(index);
    }
}
