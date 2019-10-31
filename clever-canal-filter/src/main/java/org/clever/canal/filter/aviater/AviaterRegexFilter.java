package org.clever.canal.filter.aviater;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.filter.exception.CanalFilterException;

import java.util.*;

/**
 * 基于aviater进行tableName正则匹配的过滤算法 <br/>
 * 使用Perl正则表达式进行匹配，支持多个正则表达式，多个正则表达式使用逗号(,)分隔
 * <pre>
 *  1. 所有表： ".*" 或者 ".*\\..*"
 *  2. canal schema下canal打头的表："canal\\.canal.*"
 *  3. canal schema下test表："canal\\.test"
 *  4. 多个规则："canal\\..*,mysql.test1,mysql.test2,"
 * </pre>
 */
@SuppressWarnings({"WeakerAccess"})
public class AviaterRegexFilter implements CanalEventFilter<String> {

    /**
     * 我们的配置的binlog过滤规则可以由多个正则表达式组成，使用逗号”,"进行分割
     */
    private static final String SPLIT = ",";
    /**
     * 将经过逗号",”分割后的过滤规则重新使用|串联起来
     */
    private static final String PATTERN_SPLIT = "|";
    /**
     * canal定义的Aviator过滤表达式，使用了regex自定义函数，接受pattern和target两个参数
     */
    private static final String FILTER_EXPRESSION = "regex(pattern,target)";
    /**
     * regex自定义函数实现，RegexFunction的getName方法返回regex，call方法接受两个参数
     */
    private static final RegexFunction regexFunction = new RegexFunction();
    /**
     * 用于比较两个字符串的大小
     */
    private static final Comparator<String> COMPARATOR = new StringComparator();

    static {
        // 将自定义函数添加到AviatorEvaluator中
        AviatorEvaluator.addFunction(regexFunction);
    }

    /**
     * 对自定义表达式进行编译，得到Expression对象
     */
    private final Expression exp = AviatorEvaluator.compile(FILTER_EXPRESSION, true);
    /**
     * 用户设置的过滤规则，需要使用SPLIT进行分割
     */
    private final String pattern;
    /**
     * 在没有指定过滤规则pattern情况下的默认值，例如默认为true，表示用户不指定过滤规则情况下，总是返回所有的binlog event
     */
    private final boolean defaultEmptyValue;

    public AviaterRegexFilter(String pattern) {
        this(pattern, true);
    }

    public AviaterRegexFilter(String pattern, boolean defaultEmptyValue) {
        this.defaultEmptyValue = defaultEmptyValue;
        List<String> list;
        if (StringUtils.isEmpty(pattern)) {
            list = new ArrayList<>();
        } else {
            String[] ss = StringUtils.split(pattern, SPLIT);
            list = Arrays.asList(ss);
        }
        // 对pattern按照从长到短的排序
        // 因为 foo|foot 匹配 foot 会出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot
        // 的长度不一样
        list.sort(COMPARATOR);
        // 对pattern进行头尾完全匹配
        list = completionPattern(list);
        this.pattern = StringUtils.join(list, PATTERN_SPLIT);
    }

    public boolean filter(String filtered) throws CanalFilterException {
        if (StringUtils.isEmpty(pattern)) {
            return defaultEmptyValue;
        }
        if (StringUtils.isEmpty(filtered)) {
            return defaultEmptyValue;
        }
        Map<String, Object> env = new HashMap<>();
        env.put("pattern", pattern);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

    /**
     * 修复正则表达式匹配的问题，因为使用了 oro 的 matches，会出现：
     *
     * <pre>
     * foo|foot 匹配 foot 出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot 的长度不一样
     * </pre>
     * <p>
     * 因此此类对正则表达式进行了从长到短的排序
     *
     * @author zebin.xuzb 2012-10-22 下午2:02:26
     * @version 1.0.0
     */
    private static class StringComparator implements Comparator<String> {
        @Override
        public int compare(String str1, String str2) {
            return Integer.compare(str2.length(), str1.length());
        }
    }

    /**
     * 修复正则表达式匹配的问题，即使按照长度递减排序，还是会出现以下问题：
     *
     * <pre>
     * foooo|f.*t 匹配 fooooot 出错，原因是 fooooot 匹配了 foooo 之后，会将 fooo 和数据进行匹配，但是 foooo 的长度和 fooooot 的长度不一样
     * </pre>
     * <p>
     * 因此此类对正则表达式进行头尾完全匹配
     */
    private List<String> completionPattern(List<String> patterns) {
        List<String> result = new ArrayList<>();
        for (String pattern : patterns) {
            String stringBuffer = "^" + pattern + "$";
            result.add(stringBuffer);
        }
        return result;
    }

    @Override
    public String toString() {
        return pattern;
    }
}
