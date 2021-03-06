package org.clever.canal.protocol.position;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;

import java.io.Serializable;

/**
 * 描述一个position范围 (binlog Position 范围)
 */
@Getter
@Setter
public class PositionRange<T extends Position> implements Serializable {
    private static final long serialVersionUID = -9162037079815694784L;
    /**
     * 开始位置
     */
    private T start;
    /**
     * Ack位置
     * add by ljh at 2012-09-05，用于记录一个可被ack的位置，保证每次提交到cursor中的位置是一个完整事务的结束
     */
    private T ack;
    /**
     * 结束位置
     */
    private T end;
    /**
     * add by ljh at 2019-06-25，用于精确记录ringBuffer中的位点
     */
    private Long endSeq = -1L;

    public PositionRange() {
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((ack == null) ? 0 : ack.hashCode());
        result = prime * result + ((end == null) ? 0 : end.hashCode());
        result = prime * result + ((start == null) ? 0 : start.hashCode());
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
        if (!(obj instanceof PositionRange)) {
            return false;
        }
        PositionRange other = (PositionRange) obj;
        if (ack == null) {
            if (other.ack != null) {
                return false;
            }
        } else if (!ack.equals(other.ack)) {
            return false;
        }
        if (end == null) {
            if (other.end != null) {
                return false;
            }
        } else if (!end.equals(other.end)) {
            return false;
        }
        if (start == null) {
            return other.start == null;
        } else return start.equals(other.start);
    }
}
