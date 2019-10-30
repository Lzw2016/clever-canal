package org.clever.canal.filter;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;
import org.clever.canal.common.utils.MigrateMap;

import java.util.Map;

public class PatternUtils {

    private static Map<String, Pattern> patterns = MigrateMap.makeComputingMap(
            256,
            pattern -> {
                try {
                    PatternCompiler pc = new Perl5Compiler();
                    return pc.compile(pattern, Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.READ_ONLY_MASK | Perl5Compiler.SINGLELINE_MASK);
                } catch (MalformedPatternException e) {
                    throw new RuntimeException(e);
                }
            }
    );

    public static Pattern getPattern(String pattern) {
        return patterns.get(pattern);
    }

    public static void clear() {
        patterns.clear();
    }
}
