package org.clever.canal.common.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/29 09:24 <br/>
 */
@SuppressWarnings("WeakerAccess")
public class Assert {

    public static void notNull(Object object) {
        notNull(object, "[Assertion failed] - this argument is required; it must not be null");
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void hasText(String text) {
        hasText(text, "[Assertion failed] - this String argument must have text; it must not be null, empty, or blank");
    }

    public static void hasText(String text, String message) {
        if (StringUtils.isBlank(text)) {
            throw new IllegalArgumentException(message);
        }
    }
}
