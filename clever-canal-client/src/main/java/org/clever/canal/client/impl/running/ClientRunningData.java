package org.clever.canal.client.impl.running;

import lombok.Data;

/**
 * client running状态信息
 */
@Data
public class ClientRunningData {
    private short clientId;
    private String address;
    private boolean active = true;
}
