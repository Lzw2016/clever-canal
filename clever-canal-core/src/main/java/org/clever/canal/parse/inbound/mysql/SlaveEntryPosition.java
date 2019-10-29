package org.clever.canal.parse.inbound.mysql;

import org.clever.canal.protocol.position.EntryPosition;

/**
 * slave status状态的信息
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class SlaveEntryPosition extends EntryPosition {

    private static final long serialVersionUID = 5271424551446372093L;
    private final String masterHost;
    private final String masterPort;

    public SlaveEntryPosition(String fileName, long position, String masterHost, String masterPort) {
        super(fileName, position);
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public String getMasterPort() {
        return masterPort;
    }
}
