package org.clever.canal.store.helper;

import org.apache.commons.lang3.StringUtils;
import org.clever.canal.protocol.position.EntryPosition;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.store.model.Event;

/**
 * 相关的操作工具
 */
@SuppressWarnings({"DuplicatedCode", "unused"})
public class CanalEventUtils {

    /**
     * 找出一个最小的position位置，相等的情况返回position1
     */
    public static LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPosition().getJournalName().compareTo(position2.getPosition().getJournalName()) > 0) {
                return position2;
            } else if (position1.getPosition().getJournalName().compareTo(position2.getPosition().getJournalName()) < 0) {
                return position1;
            } else {
                // 根据offset进行比较
                if (position1.getPosition().getPosition() > position2.getPosition().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPosition().getTimestamp() > position2.getPosition().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    /**
     * 根据entry创建对应的Position对象
     */
    public static LogPosition createPosition(Event event) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getJournalName());
        position.setPosition(event.getPosition());
        position.setTimestamp(event.getExecuteTime());
        // add serverId at 2016-06-28
        position.setServerId(event.getServerId());
        // add gtId
        position.setGtId(event.getGtId());

        LogPosition logPosition = new LogPosition();
        logPosition.setPosition(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 根据entry创建对应的Position对象
     */
    public static LogPosition createPosition(Event event, boolean included) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getJournalName());
        position.setPosition(event.getPosition());
        position.setTimestamp(event.getExecuteTime());
        position.setIncluded(included);
        // add serverId at 2016-06-28
        position.setServerId(event.getServerId());
        // add gtId
        position.setGtId(event.getGtId());

        LogPosition logPosition = new LogPosition();
        logPosition.setPosition(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 判断当前的entry和position是否相同
     */
    public static boolean checkPosition(Event event, LogPosition logPosition) {
        EntryPosition position = logPosition.getPosition();
        boolean result = position.getTimestamp().equals(event.getExecuteTime());
        boolean exactly = (StringUtils.isBlank(position.getJournalName()) && position.getPosition() == null);
        // 精确匹配
        if (!exactly) {
            result &= position.getPosition().equals(event.getPosition());
            if (result) {
                // short path
                result = StringUtils.equals(event.getJournalName(), position.getJournalName());
            }
        }
        return result;
    }
}
