package org.clever.canal.server.netty;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.clever.canal.protocol.SecurityUtil;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/07 16:39 <br/>
 */
@Slf4j
public class SecurityUtilTest {

    @Test
    public void t1() throws NoSuchAlgorithmException {
        String password = "12121";
        final byte[] seed = RandomUtils.nextBytes(8);
        String password_1 = SecurityUtil.byte2HexStr(SecurityUtil.scramble411(password.getBytes(), seed));
        String password_2 = SecurityUtil.scrambleGenPass(password.getBytes());
        boolean flag = false;
        try {
            byte[] passForClient = SecurityUtil.hexStr2Bytes(password_1);
            flag = SecurityUtil.scrambleServerAuth(passForClient, SecurityUtil.hexStr2Bytes(password_2), seed);
        } catch (NoSuchAlgorithmException e) {
            log.warn(e.getMessage(), e);
        }
        log.info("### --> {}", flag);
    }

    @Test
    public void testSimple() throws NoSuchAlgorithmException {
        final byte[] seed = RandomUtils.nextBytes(8);
        // String str = "e3619321c1a937c46a0d8bd1dac39f93b27d4458"; // canal password
        String str = SecurityUtil.scrambleGenPass("canal".getBytes());
        byte[] client = SecurityUtil.scramble411("canal".getBytes(), seed);
        boolean check = SecurityUtil.scrambleServerAuth(client, SecurityUtil.hexStr2Bytes(str), seed);
        log.info("### --> {}", check);
    }
}
