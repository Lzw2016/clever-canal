package org.clever.canal.protocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;

import java.io.Serializable;

/**
 * 客户端标识
 */
@SuppressWarnings("unused")
public class ClientIdentity implements Serializable {
    private static final long serialVersionUID = -8262100681930834834L;

    /**
     * 客户端需要订阅的数据源名称
     */
    private String destination;
    /**
     * 客户端ID
     */
    private short clientId;
    /**
     * 客户端过滤配置
     */
    private String filter;

    public ClientIdentity() {
    }

    /**
     * @param destination 客户端需要订阅的数据源名称
     * @param clientId    客户端ID
     */
    public ClientIdentity(String destination, short clientId) {
        this.clientId = clientId;
        this.destination = destination;
    }

    /**
     * @param destination 客户端需要订阅的数据源名称
     * @param clientId    客户端ID
     * @param filter      客户端过滤配置
     */
    public ClientIdentity(String destination, short clientId, String filter) {
        this.clientId = clientId;
        this.destination = destination;
        this.filter = filter;
    }

    /**
     * 是否有过滤规则配置
     */
    public Boolean hasFilter() {
        return StringUtils.isNotBlank(filter);
    }

    // ======== setter =========

    public String getDestination() {
        return destination;
    }

    public short getClientId() {
        return clientId;
    }

    public void setClientId(short clientId) {
        this.clientId = clientId;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + clientId;
        result = prime * result + ((destination == null) ? 0 : destination.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ClientIdentity)) {
            return false;
        }
        ClientIdentity other = (ClientIdentity) obj;
        if (clientId != other.clientId) {
            return false;
        }
        if (destination == null) {
            return other.destination == null;
        } else return destination.equals(other.destination);
    }
}
