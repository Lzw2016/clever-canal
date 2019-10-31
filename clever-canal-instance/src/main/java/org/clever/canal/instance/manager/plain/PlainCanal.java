package org.clever.canal.instance.manager.plain;

import java.util.Properties;

/**
 * plain远程配置，提供基于properties纯文本的配置
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class PlainCanal {

    private Properties properties;
    private String md5;
    private String status;

    public PlainCanal() {
    }

    public PlainCanal(Properties properties, String status, String md5) {
        this.properties = properties;
        this.md5 = md5;
        this.status = status;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "PlainCanal [properties=" + properties + ", md5=" + md5 + ", status=" + status + "]";
    }
}
