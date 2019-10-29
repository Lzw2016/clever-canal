package org.clever.canal.filter.aviater;

import com.googlecode.aviator.AviatorEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.filter.exception.CanalFilterException;
import org.clever.canal.protocol.CanalEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于aviater el表达式的匹配过滤
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class AviaterELFilter implements CanalEventFilter<CanalEntry.Entry> {

    public static final String ROOT_KEY = "entry";
    private String expression;

    public AviaterELFilter(String expression) {
        this.expression = expression;
    }

    public boolean filter(CanalEntry.Entry entry) throws CanalFilterException {
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Map<String, Object> env = new HashMap<>();
        env.put(ROOT_KEY, entry);
        return (Boolean) AviatorEvaluator.execute(expression, env);
    }
}
