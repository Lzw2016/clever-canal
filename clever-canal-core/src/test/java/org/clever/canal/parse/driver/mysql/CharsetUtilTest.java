package org.clever.canal.parse.driver.mysql;

import org.clever.canal.parse.driver.mysql.utils.CharsetUtil;
import org.junit.Assert;
import org.junit.Test;

public class CharsetUtilTest {

    @Test
    public void testLatin1() {
        int charsetIndex = 5;
        String charset = "latin1";
        Assert.assertEquals(charset, CharsetUtil.getCharset(charsetIndex));
    }

    @Test
    public void testGbk() {
        int charsetIndex = 87;
        String charset = "gbk";
        Assert.assertEquals(charset, CharsetUtil.getCharset(charsetIndex));
    }

    @Test
    public void testGb2312() {
        int charsetIndex = 24;
        String charset = "gb2312";
        Assert.assertEquals(charset, CharsetUtil.getCharset(charsetIndex));
    }

    @Test
    public void testUtf8() {
        int charsetIndex = 213;
        String charset = "utf8";
        Assert.assertEquals(charset, CharsetUtil.getCharset(charsetIndex));
    }

    @Test
    public void testUtf8mb4() {
        int charsetIndex = 235;
        String charset = "utf8mb4";
        Assert.assertEquals(charset, CharsetUtil.getCharset(charsetIndex));
    }

    @Test
    public void testBinary() {
        int charsetIndex = 63;
        String charset = "binary";
        Assert.assertEquals(charset, CharsetUtil.getCharset(charsetIndex));
    }
}
