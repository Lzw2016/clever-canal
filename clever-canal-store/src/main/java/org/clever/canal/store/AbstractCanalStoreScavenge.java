package org.clever.canal.store;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.utils.CollectionUtils;
import org.clever.canal.meta.CanalMetaManager;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.protocol.position.Position;

import java.util.List;

/**
 * store回收机制
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class AbstractCanalStoreScavenge extends AbstractCanalLifeCycle implements CanalStoreScavenge {

    protected String destination;
    protected CanalMetaManager canalMetaManager;
    protected boolean onAck = true;
    protected boolean onFull = false;
    protected boolean onSchedule = false;
    protected String scavengeSchedule = null;

    public void scavenge() {
        Position position = getLatestAckPosition(destination);
        cleanUntil(position);
    }

    /**
     * 找出该destination中可被清理掉的position位置
     */
    private Position getLatestAckPosition(String destination) {
        List<ClientIdentity> clientIdentities = canalMetaManager.listAllSubscribeInfo(destination);
        LogPosition result = null;
        if (!CollectionUtils.isEmpty(clientIdentities)) {
            // 尝试找到一个最小的logPosition
            for (ClientIdentity clientIdentity : clientIdentities) {
                LogPosition position = (LogPosition) canalMetaManager.getCursor(clientIdentity);
                if (position == null) {
                    continue;
                }
                if (result == null) {
                    result = position;
                } else {
                    result = min(result, position);
                }
            }
        }
        return result;
    }

    /**
     * 找出一个最小的position位置
     */
    @SuppressWarnings("DuplicatedCode")
    private LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPosition().getJournalName().compareTo(position2.getPosition().getJournalName()) < 0) {
                return position2;
            } else if (position1.getPosition().getJournalName().compareTo(position2.getPosition().getJournalName()) > 0) {
                return position1;
            } else {
                // 根据offset进行比较
                if (position1.getPosition().getPosition() < position2.getPosition().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPosition().getTimestamp() < position2.getPosition().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    public void setOnAck(boolean onAck) {
        this.onAck = onAck;
    }

    public void setOnFull(boolean onFull) {
        this.onFull = onFull;
    }

    public void setOnSchedule(boolean onSchedule) {
        this.onSchedule = onSchedule;
    }

    public String getScavengeSchedule() {
        return scavengeSchedule;
    }

    public void setScavengeSchedule(String scavengeSchedule) {
        this.scavengeSchedule = scavengeSchedule;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setCanalMetaManager(CanalMetaManager canalMetaManager) {
        this.canalMetaManager = canalMetaManager;
    }
}
