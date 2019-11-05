package org.clever.canal.instance.manager.model;

/**
 * CanalLogPositionManager 的存储模式
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:10 <br/>
 */
public enum LogPositionMode {
    /**
     * 内存存储模式
     */
    MEMORY,
    /**
     * 基于meta信息
     */
    META,
    /**
     * 基于内存+meta的failBack实现
     */
    MEMORY_META_FAIL_BACK,
//        /**
//         * 文件存储模式 zookeeper
//         */
//        ZOOKEEPER,
//        /**
//         * 混合模式，内存+文件
//         */
//        MIXED,
}
