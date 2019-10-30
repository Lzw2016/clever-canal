package org.clever.canal.common.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/30 15:48 <br/>
 */
public class MigrateMap {

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(int maxInitialCapacity, Function<? super K, ? extends V> mappingFunction) {
        return new ComputingConcurrentHashMap<>(maxInitialCapacity, mappingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
        return new ComputingConcurrentHashMap<>(computingFunction);
    }

    @SuppressWarnings("WeakerAccess")
    public static final class ComputingConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

        private final int maxInitialCapacity;
        private final Function<? super K, ? extends V> mappingFunction;

        public ComputingConcurrentHashMap(int maxInitialCapacity, Function<? super K, ? extends V> mappingFunction) {
            super(-1);
            Assert.notNull(mappingFunction);
            this.mappingFunction = mappingFunction;
            this.maxInitialCapacity = maxInitialCapacity;
        }

        public ComputingConcurrentHashMap(Function<? super K, ? extends V> mappingFunction) {
            super(-1);
            Assert.notNull(mappingFunction);
            this.mappingFunction = mappingFunction;
            maxInitialCapacity = -1;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(Object key) {
            V value = super.get(key);
            if (value == null) {
                K keyTmp = (K) key;
                if (maxInitialCapacity > 0 && this.size() > maxInitialCapacity) {
                    this.clear();
                }
                return super.computeIfAbsent(keyTmp, mappingFunction);
            }
            return value;
        }
    }
}
