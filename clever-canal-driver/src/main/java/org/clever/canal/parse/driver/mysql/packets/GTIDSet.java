package org.clever.canal.parse.driver.mysql.packets;

import java.io.IOException;

public interface GTIDSet {

    /**
     * 序列化成字节数组
     */
    byte[] encode() throws IOException;

    /**
     * 更新当前实例
     */
    void update(String str);
}
