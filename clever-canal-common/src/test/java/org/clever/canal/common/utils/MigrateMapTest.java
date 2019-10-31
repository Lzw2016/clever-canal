package org.clever.canal.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/10/31 09:42 <br/>
 */
@Slf4j
public class MigrateMapTest {

    @Test
    public void t1() {
        ConcurrentMap<String, String> concurrentMap = MigrateMap.makeComputingMap(str -> str);
        log.info("### {}", concurrentMap.get("111"));
        log.info("### {}", concurrentMap.get("222"));
        log.info("### {}", concurrentMap.get("111"));
        log.info("### {}", concurrentMap.get("333"));
    }
}
