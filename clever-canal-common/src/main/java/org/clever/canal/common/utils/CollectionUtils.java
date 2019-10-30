package org.clever.canal.common.utils;

import java.util.Collection;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/29 09:22 <br/>
 */
public class CollectionUtils {
    public static boolean isEmpty(Collection collection) {
        return (collection == null || collection.isEmpty());
    }
}
