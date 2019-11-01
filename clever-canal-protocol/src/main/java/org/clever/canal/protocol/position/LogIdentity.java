package org.clever.canal.protocol.position;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;

import java.net.InetSocketAddress;

/**
 * log数据产生的来源
 */
@SuppressWarnings("unused")
public class LogIdentity extends Position {
    private static final long serialVersionUID = 5530225131455662581L;

    /**
     * 链接服务器的地址
     */
    private InetSocketAddress sourceAddress;
    /**
     * 对应的slaveId(注：这个值没有意义，赋值永远是-1，没有读取) <br/>
     * 设计的本意是为了实现一个canalInstance对于多个canalClient消费
     */
    private Long slaveId;

    public LogIdentity() {
    }

    public LogIdentity(InetSocketAddress sourceAddress, Long slaveId) {
        this.sourceAddress = sourceAddress;
        this.slaveId = slaveId;
    }

    public InetSocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public void setSourceAddress(InetSocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public Long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((slaveId == null) ? 0 : slaveId.hashCode());
        result = prime * result + ((sourceAddress == null) ? 0 : sourceAddress.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LogIdentity other = (LogIdentity) obj;
        if (slaveId == null) {
            if (other.slaveId != null) return false;
        } else if (slaveId != (other.slaveId.longValue())) return false;
        if (sourceAddress == null) {
            return other.sourceAddress == null;
        } else return sourceAddress.equals(other.sourceAddress);
    }
}
