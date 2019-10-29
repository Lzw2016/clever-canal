package org.clever.canal.instance.manager.plain;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.CanalException;
import org.clever.canal.common.CanalLifeCycle;
import org.clever.canal.protocol.SecurityUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 远程配置获取
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class PlainCanalConfigClient extends AbstractCanalLifeCycle implements CanalLifeCycle {

    private final static Integer REQUEST_TIMEOUT = 5000;
    private String configURL;
    private String user;
    private String passwd;
    private HttpHelper httpHelper;
    private String localIp;
    private int adminPort;
    private boolean autoRegister;
    private String autoCluster;

    public PlainCanalConfigClient(String configURL, String user, String passwd, String localIp, int adminPort, boolean autoRegister, String autoCluster) {
        this(configURL, user, passwd, localIp, adminPort);
        this.autoCluster = autoCluster;
        this.autoRegister = autoRegister;
    }

    public PlainCanalConfigClient(String configURL, String user, String passwd, String localIp, int adminPort) {
        this.configURL = configURL;
        if (!StringUtils.startsWithIgnoreCase(configURL, "http")) {
            this.configURL = "http://" + configURL;
        } else {
            this.configURL = configURL;
        }
        this.user = user;
        this.passwd = passwd;
        this.httpHelper = new HttpHelper();
        if (StringUtils.isEmpty(localIp)) {
            // 本地测试用
            this.localIp = "127.0.0.1";
        } else {
            this.localIp = localIp;
        }
        this.adminPort = adminPort;
    }

    /**
     * 加载canal.properties文件
     *
     * @return 远程配置的properties
     */
    public PlainCanal findServer(String md5) {
        if (StringUtils.isEmpty(md5)) {
            md5 = "";
        }
        String url = configURL + "/api/v1/config/server_polling?ip=" + localIp + "&port=" + adminPort + "&md5=" + md5
                + "&register=" + (autoRegister ? 1 : 0) + "&cluster=" + autoCluster;
        return queryConfig(url);
    }

    /**
     * 加载远程的instance.properties
     */
    public PlainCanal findInstance(String destination, String md5) {
        if (StringUtils.isEmpty(md5)) {
            md5 = "";
        }
        String url = configURL + "/api/v1/config/instance_polling/" + destination + "?md5=" + md5;
        return queryConfig(url);
    }

    /**
     * 返回需要运行的instance列表
     */
    public String findInstances(String md5) {
        if (StringUtils.isEmpty(md5)) {
            md5 = "";
        }
        String url = configURL + "/api/v1/config/instances_polling?md5=" + md5 + "&ip=" + localIp + "&port="
                + adminPort;
        ResponseModel<CanalConfig> config = doQuery(url);
        if (config.data != null) {
            return config.data.content;
        } else {
            return null;
        }
    }

    private PlainCanal queryConfig(String url) {
        try {
            ResponseModel<CanalConfig> config = doQuery(url);
            return processData(config.data);
        } catch (Throwable e) {
            throw new CanalException("load manager config failed.", e);
        }
    }

    private ResponseModel<CanalConfig> doQuery(String url) {
        Map<String, String> heads = new HashMap<>();
        heads.put("user", user);
        heads.put("passwd", passwd);
        String response = httpHelper.get(url, heads, REQUEST_TIMEOUT);
        ResponseModel<CanalConfig> resp = JSONObject.parseObject(
                response,
                new TypeReference<ResponseModel<CanalConfig>>() {
                }
        );
        if (!HttpHelper.REST_STATE_OK.equals(resp.code)) {
            throw new CanalException("requestGet for canal config error: " + resp.message);
        }
        return resp;
    }

    private PlainCanal processData(CanalConfig config) throws IOException, NoSuchAlgorithmException {
        Properties properties = new Properties();
        String md5;
        String status;
        if (config != null && StringUtils.isNotEmpty(config.content)) {
            md5 = SecurityUtil.md5String(config.content);
            status = config.status;
            properties.load(new ByteArrayInputStream(config.content.getBytes(StandardCharsets.UTF_8)));
        } else {
            // null代表没有新配置变更
            return null;
        }
        return new PlainCanal(properties, status, md5);
    }

    private static class ResponseModel<T> {
        public Integer code;
        public String message;
        public T data;
    }

    @SuppressWarnings("WeakerAccess")
    private static class CanalConfig {
        public String content;
        public String status;
    }
}
