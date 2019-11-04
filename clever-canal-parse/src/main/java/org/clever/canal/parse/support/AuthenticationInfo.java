package org.clever.canal.parse.support;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.clever.canal.parse.utils.ConfigTools;

import java.net.InetSocketAddress;

/**
 * 数据库认证信息
 */
@Getter
@Setter
public class AuthenticationInfo {
    /**
     * 主库信息
     */
    private InetSocketAddress address;
    /**
     * 帐号
     */
    private String username;
    /**
     * 密码
     */
    private String password;
    /**
     * 默认链接的数据库
     */
    private String defaultDatabaseName;
    /**
     * 公钥
     */
    private String pwdPublicKey;
    /**
     * 是否使用druid加密解密数据库密码
     */
    private boolean enableDruid;

    @SuppressWarnings("unused")
    public void initPwd() throws Exception {
        if (enableDruid) {
            this.password = ConfigTools.decrypt(pwdPublicKey, password);
        }
    }

    public AuthenticationInfo() {
    }

    @SuppressWarnings("unused")
    public AuthenticationInfo(InetSocketAddress address, String username, String password) {
        this(address, username, password, "");
    }

    public AuthenticationInfo(InetSocketAddress address, String username, String password, String defaultDatabaseName) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.defaultDatabaseName = defaultDatabaseName;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + ((defaultDatabaseName == null) ? 0 : defaultDatabaseName.hashCode());
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
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
        if (!(obj instanceof AuthenticationInfo)) {
            return false;
        }
        AuthenticationInfo other = (AuthenticationInfo) obj;
        if (address == null) {
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        if (defaultDatabaseName == null) {
            if (other.defaultDatabaseName != null) {
                return false;
            }
        } else if (!defaultDatabaseName.equals(other.defaultDatabaseName)) {
            return false;
        }
        if (password == null) {
            if (other.password != null) {
                return false;
            }
        } else if (!password.equals(other.password)) {
            return false;
        }
        if (username == null) {
            return other.username == null;
        } else return username.equals(other.username);
    }
}
