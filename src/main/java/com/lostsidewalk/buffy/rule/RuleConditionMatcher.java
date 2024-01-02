package com.lostsidewalk.buffy.rule;

import com.lostsidewalk.buffy.post.ContentObject;
import com.lostsidewalk.buffy.post.StagingPost;
import com.lostsidewalk.buffy.rule.RuleCondition.ComparisonType;
import com.lostsidewalk.buffy.rule.RuleCondition.FieldName;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@SuppressWarnings("WeakerAccess")
@Slf4j
@Component
class RuleConditionMatcher {

    @Autowired
    RuleComparator ruleComparator;

    final boolean evaluateStagingPost(Rule rule, StagingPost stagingPost) {
        long ruleId = rule.getId();
        boolean isMatch;
        Rule.MatchType matchType = rule.getMatchType();
        if (matchType == Rule.MatchType.ANY) {
            // if <field> <comparison> <value> || if <field> <comparison> <value> || ...
            isMatch = rule.getConditions().stream()
                    .anyMatch(condition -> matches(ruleId, condition.getFieldName(), condition.getComparisonType(), condition.getFieldValue(), stagingPost));
        } else { // MatchType.ALL, default
            // if <field> <comparison> <value> && if <field> <comparison> <value> && ...
            isMatch = rule.getConditions().stream()
                    .allMatch(condition -> matches(ruleId, condition.getFieldName(), condition.getComparisonType(), condition.getFieldValue(), stagingPost));
        }
        return isMatch;
    }

    private boolean matches(long ruleId, FieldName fieldName, ComparisonType comparisonType, Object fieldValue, StagingPost stagingPost) {
        log.debug("Evaluating match condition, ruleId={}, fieldName={}, comparisonType={}, fieldValue={}, postHash={}",
                ruleId, fieldName, comparisonType, fieldValue, stagingPost.getPostHash());
        boolean isMatch = false;
        String sourceValue = (fieldValue == null ? EMPTY : fieldValue.toString());
        switch (fieldName) {
            case TITLE -> {
                String targetValue = ofNullable(stagingPost.getPostTitle())
                        .map(ContentObject::getValue)
                        .orElse(EMPTY);
                isMatch = ruleComparator.makeComparison(comparisonType, sourceValue, targetValue);
            }
            case DESCRIPTION -> {
                String targetValue = ofNullable(stagingPost.getPostDesc())
                        .map(ContentObject::getValue)
                        .orElse(EMPTY);
                isMatch = ruleComparator.makeComparison(comparisonType, sourceValue, targetValue);
            }
            case CONTENTS -> {
                String targetValue = ofNullable(stagingPost.getPostContents())
                        .filter(ObjectUtils::isNotEmpty)
                        .map(postContents -> postContents.get(0))
                        .map(ContentObject::getValue)
                        .orElse(EMPTY);
                isMatch = ruleComparator.makeComparison(comparisonType, sourceValue, targetValue);
            }
        }
        log.debug("Match condition result={}, ruleId={}, fieldName={}, comparisonType={}, fieldValue={}, postHash={}",
                isMatch, ruleId, fieldName, comparisonType, fieldValue, stagingPost.getPostHash());
        return isMatch;
    }

    @Override
    public final String toString() {
        return "RuleConditionMatcher{" +
                "ruleComparator=" + ruleComparator +
                '}';
    }
}
