package org.clever.canal.server.netty;

import org.clever.canal.protocol.CanalPacket;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 20:00 <br/>
 */
public interface NettyServerConstant {
    /**
     * 服务端版本号
     */
    int VERSION = 1;
    /**
     *
     */
    int HEADER_LENGTH = 4;
    /**
     * 使用的压缩算法
     */
    CanalPacket.Compression COMPRESSION = CanalPacket.Compression.NONE;
}
