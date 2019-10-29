package org.clever.canal.instance.core;

@SuppressWarnings("unused")
public interface CanalInstanceGenerator {

    /**
     * 通过 destination 产生特定的 {@link CanalInstance}
     */
    CanalInstance generate(String destination);
}
