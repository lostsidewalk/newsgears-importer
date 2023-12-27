package com.lostsidewalk.buffy.rule;

import com.lostsidewalk.buffy.rule.RuleCondition.ComparisonType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import static java.util.regex.Pattern.compile;
import static org.apache.commons.lang3.StringUtils.*;
import static org.apache.commons.lang3.StringUtils.endsWith;

/**
 * Component responsible for implementing the various comparison operations supported by the rules engine.
 */
@Slf4j
@Component
public class RuleComparator {

    /**
     * Default constructor; initialized the object.
     */
    RuleComparator() {
    }

    @SuppressWarnings("MethodMayBeStatic")
    final boolean makeComparison(ComparisonType comparisonType, String sourceValue, String targetValue) {
        String s = trim(sourceValue);
        String t = trim(targetValue);
        switch (comparisonType) {
            case EQ_LITERAL -> {
                return StringUtils.equals(s, t);
            }
            case EQ_REGEXP -> {
                return compile(s).matcher(t).matches();
            }
            case CONTAINS -> {
                return contains(s, t);
            }
            case STARTS_WITH -> {
                return startsWith(s, t);
            }
            case ENDS_WITH -> {
                return endsWith(s, t);
            }
        }
        return false;
    }
}
