package org.clever.canal.filter.aviater;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.filter.exception.CanalFilterException;

import java.util.*;

/**
 * 基于aviater进行tableName简单过滤计算，不支持正则匹配
 */
@SuppressWarnings("unused")
public class AviaterSimpleFilter implements CanalEventFilter<String> {

    private static final String SPLIT = ",";

    private static final String FILTER_EXPRESSION = "include(list,target)";

    private final Expression exp = AviatorEvaluator.compile(FILTER_EXPRESSION, true);

    private final List<String> list;

    public AviaterSimpleFilter(String filterExpression) {
        if (StringUtils.isEmpty(filterExpression)) {
            list = new ArrayList<>();
        } else {
            String[] ss = filterExpression.toLowerCase().split(SPLIT);
            list = Arrays.asList(ss);
        }
    }

    public boolean filter(String filtered) throws CanalFilterException {
        if (list.isEmpty()) {
            return true;
        }
        if (StringUtils.isEmpty(filtered)) {
            return true;
        }
        Map<String, Object> env = new HashMap<>();
        env.put("list", list);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }
}
