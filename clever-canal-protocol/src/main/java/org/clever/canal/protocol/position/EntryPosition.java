package org.clever.canal.protocol.position;

/**
 * 数据库对象的唯一标示（binlog位置信息）
 */
@SuppressWarnings({"unused", "BooleanMethodIsAlwaysInverted"})
public class EntryPosition extends TimePosition {
    private static final long serialVersionUID = 81432665066427482L;

    /**
     * 包含标识(MemoryEventStoreWithBuffer 使用)
     */
    private boolean included = false;
    /**
     * binlog 文件名称(如：mysql-bin.000027)
     */
    private String journalName;
    /**
     * 读取 binlog 文件位置
     */
    private Long position;
    /**
     * Mysql Server Id (记录一下位点对应的serverId)<br />
     * add by agapple at 2016-06-28
     */
    private Long serverId = null;
    /**
     * 全局事务ID
     */
    private String gtId = null;

    public EntryPosition() {
        super(null);
    }

    public EntryPosition(Long timestamp) {
        this(null, null, timestamp);
    }

    public EntryPosition(String journalName, Long position) {
        this(journalName, position, null);
    }

    public EntryPosition(String journalName, Long position, Long timestamp) {
        super(timestamp);
        this.journalName = journalName;
        this.position = position;
    }

    public EntryPosition(String journalName, Long position, Long timestamp, Long serverId) {
        this(journalName, position, timestamp);
        this.serverId = serverId;
    }

    public String getJournalName() {
        return journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public boolean isIncluded() {
        return included;
    }

    public void setIncluded(boolean included) {
        this.included = included;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

    public String getGtId() {
        return gtId;
    }

    public void setGtId(String gtId) {
        this.gtId = gtId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((journalName == null) ? 0 : journalName.hashCode());
        result = prime * result + ((position == null) ? 0 : position.hashCode());
        // 手写equals，自动生成时需注意
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof EntryPosition)) {
            return false;
        }
        EntryPosition other = (EntryPosition) obj;
        if (journalName == null) {
            if (other.journalName != null) {
                return false;
            }
        } else if (!journalName.equals(other.journalName)) {
            return false;
        }
        if (position == null) {
            if (other.position != null) {
                return false;
            }
        } else if (!position.equals(other.position)) {
            return false;
        }
        // 手写equals，自动生成时需注意
        if (timestamp == null) {
            return other.timestamp == null;
        } else return timestamp.equals(other.timestamp);
    }

    /**
     * {@inheritDoc}
     *
     * @see Comparable#compareTo(Object)
     */
    public int compareTo(EntryPosition o) {
        final int val = journalName.compareTo(o.journalName);

        if (val == 0) {
            return (int) (position - o.position);
        }
        return val;
    }
}
