package org.clever.canal.protocol;

import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Getter
@Setter
public class Message implements Serializable {
    private static final long serialVersionUID = 1234034768477580009L;
    /**
     * 消息ID(值为-1表示为空数据)
     */
    private final long id;
    /**
     * 已经解析了的消息数据
     */
    private List<CanalEntry.Entry> entries = Collections.emptyList();
    /**
     * 原始消息数据,未解析的消息数据 <br />
     * row data for performance, see: https://github.com/alibaba/canal/issues/726
     */
    private List<ByteString> rawEntries = Collections.emptyList();
    /**
     * 消息数据是否是原始数据
     */
    private final boolean raw;

    /**
     * 空数据
     *
     * @param id  消息ID
     * @param raw 消息数据是否是原始数据
     */
    public Message(long id, boolean raw) {
        this.id = id;
        this.raw = raw;
    }

    /**
     * raw = false
     *
     * @param id      消息ID
     * @param entries 已经解析了的消息数据
     */
    public Message(long id, List<CanalEntry.Entry> entries) {
        if (entries == null) {
            entries = Collections.emptyList();
        }
        this.id = id;
        this.entries = entries;
        this.raw = false;
    }

    /**
     * raw = true
     *
     * @param entries    已经解析了的消息数据
     * @param rawEntries 原始消息数据
     * @param id         消息ID
     */
    public Message(long id, List<CanalEntry.Entry> entries, List<ByteString> rawEntries) {
        boolean raw = true;
        if (entries != null && !entries.isEmpty()) {
            // entries 有内容就不是原始数据
            raw = false;
        }
        if (raw && rawEntries == null && entries != null) {
            // rawEntries是null, entries不是null --> 就不是原始数据
            raw = false;
        }
        if (entries == null) {
            entries = Collections.emptyList();
        }
        if (rawEntries == null) {
            rawEntries = Collections.emptyList();
        }
        this.id = id;
        this.entries = entries;
        this.rawEntries = rawEntries;
        this.raw = raw;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
