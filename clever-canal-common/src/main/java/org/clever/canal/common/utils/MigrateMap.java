package org.clever.canal.common.utils;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ConcurrentMap;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/30 15:48 <br/>
 */
public class MigrateMap {

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(CacheBuilder<Object, Object> cacheBuilder, CacheLoader<K, V> loader) {
        return cacheBuilder.build(loader).asMap();
    }

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<K, V> computingFunction) {
        return CacheBuilder.newBuilder().build(CacheLoader.from(computingFunction)).asMap();
    }

    public static <K, V> LoadingCache<K, V> makeComputingMap(CacheLoader<? super K, V> loader) {
        return CacheBuilder.newBuilder().build(loader);
    }
}
