package org.clever.canal.protocol.position;

import lombok.Getter;
import lombok.Setter;

/**
 * 基于时间的位置，position数据不唯一
 */
@Setter
@Getter
public class TimePosition extends Position {
    private static final long serialVersionUID = 6185261261064226380L;
    /**
     * 时间搓
     */
    protected Long timestamp;

    TimePosition(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TimePosition)) {
            return false;
        }
        TimePosition other = (TimePosition) obj;
        if (timestamp == null) {
            return other.timestamp == null;
        } else return timestamp.equals(other.timestamp);
    }
}
