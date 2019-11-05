package org.clever.canal.instance.manager.model;

/**
 * MetaManager 的存储模式
 * <p>
 * 作者：lizw <br/>
 * 创建时间：2019/11/05 18:07 <br/>
 */
public enum MetaMode {
    /**
     * 内存存储模式
     */
    MEMORY,
    /**
     * 本地文件存储模式(内存 + 本地文件存储)
     */
    LOCAL_FILE,
//        /**
//         * 文件存储模式
//         */
//        ZOOKEEPER,
//        /**
//         * 混合模式，内存+文件
//         */
//        MIXED;
}
