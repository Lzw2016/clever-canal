package org.clever.canal.store.model;

import com.google.protobuf.ByteString;
import lombok.Getter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;
import org.clever.canal.protocol.CanalEntry;
import org.clever.canal.protocol.CanalEntry.EntryType;
import org.clever.canal.protocol.CanalEntry.EventType;
import org.clever.canal.protocol.position.LogIdentity;

import java.io.Serializable;
import java.util.List;

/**
 * store存储数据对象
 */
@Getter
public class Event implements Serializable {
    private static final long serialVersionUID = 1333330351758762739L;

    /**
     * 记录数据产生的来源
     */
    private LogIdentity logIdentity;
    /**
     * 原始数据(未解析的数据)
     */
    private ByteString rawEntry;
    /**
     * 执行时间
     */
    private long executeTime;
    /**
     * 打散后的事件类型，主要用于标识事务的开始，变更数据，结束
     */
    private EntryType entryType;
    /**
     * binlog/redolog 文件名
     */
    private String journalName;
    /**
     * binlog/redolog 文件的偏移位置
     */
    private long position;
    /**
     * 服务端serverId
     */
    private long serverId;
    /**
     * 事件类型
     */
    private EventType eventType;
    /**
     * 当前事务的(全局事务ID) gtId
     */
    private String gtId;
    /**
     * 原始数据长度
     */
    private long rawLength;
    /**
     * 数据行数
     */
    private int rowsCount;
    /**
     * 解析binlog数据对应的对象 <br/>
     * https://github.com/alibaba/canal/issues/1019
     */
    private CanalEntry.Entry entry;

    public Event() {
    }

    public Event(LogIdentity logIdentity, CanalEntry.Entry entry) {
        this(logIdentity, entry, true);
    }

    /**
     * @param logIdentity log数据产生的来源
     * @param entry       解析binlog数据对应的实体
     * @param raw         是否以原始数据的方式保存(保存原始数据为了方便网络传输)
     */
    public Event(LogIdentity logIdentity, CanalEntry.Entry entry, boolean raw) {
        this.logIdentity = logIdentity;
        this.entryType = entry.getEntryType();
        this.executeTime = entry.getHeader().getExecuteTime();
        this.journalName = entry.getHeader().getLogfileName();
        this.position = entry.getHeader().getLogfileOffset();
        this.serverId = entry.getHeader().getServerId();
        this.gtId = entry.getHeader().getGtId();
        this.eventType = entry.getHeader().getEventType();
        if (entryType == EntryType.ROW_DATA) {
            List<CanalEntry.Pair> props = entry.getHeader().getPropsList();
            if (props != null) {
                for (CanalEntry.Pair p : props) {
                    if ("rowsCount".equals(p.getKey())) {
                        rowsCount = Integer.parseInt(p.getValue());
                        break;
                    }
                }
            }
        }
        if (raw) {
            // build raw
            this.rawEntry = entry.toByteString();
            this.rawLength = rawEntry.size();
        } else {
            this.entry = entry;
            // 按照6倍的event length预估
            this.rawLength = entry.getHeader().getEventLength() * 6;
        }
    }

    public void clearData() {
        this.entry = null;
        this.rawEntry = null;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
