package org.clever.canal.parse;

import org.clever.canal.parse.support.AuthenticationInfo;

/**
 * 支持可切换的数据复制控制器
 */
public interface CanalHASwitchable {

    void doSwitch();

    void doSwitch(AuthenticationInfo newAuthenticationInfo);
}
