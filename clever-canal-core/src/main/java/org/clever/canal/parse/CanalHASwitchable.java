package org.clever.canal.parse;

import sun.net.www.protocol.http.AuthenticationInfo;

/**
 * 支持可切换的数据复制控制器
 */
public interface CanalHASwitchable {

    void doSwitch();

    void doSwitch(AuthenticationInfo newAuthenticationInfo);
}
