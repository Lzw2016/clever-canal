package org.clever.canal.protocol.position;

import lombok.Getter;
import lombok.Setter;

/**
 * 基于mysql/oracle log位置标识
 */
@Getter
@Setter
public class LogPosition extends Position {
    private static final long serialVersionUID = 3875012010277005819L;
    /**
     * 记录数据产生的来源(MySql服务器地址 和 消费binlog的客户端ID)
     */
    private LogIdentity identity;
    /**
     * binlog 位置信息
     */
    private EntryPosition position;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identity == null) ? 0 : identity.hashCode());
        result = prime * result + ((position == null) ? 0 : position.hashCode());
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
        if (!(obj instanceof LogPosition)) {
            return false;
        }
        LogPosition other = (LogPosition) obj;
        if (identity == null) {
            if (other.identity != null) {
                return false;
            }
        } else if (!identity.equals(other.identity)) {
            return false;
        }
        if (position == null) {
            return other.position == null;
        } else return position.equals(other.position);
    }
}
