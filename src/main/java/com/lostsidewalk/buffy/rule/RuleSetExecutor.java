package com.lostsidewalk.buffy.rule;

import com.lostsidewalk.buffy.post.StagingPost;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;

import static org.apache.commons.collections4.CollectionUtils.isEmpty;

/**
 * Component responsible for executing rules against staging posts.
 */
@SuppressWarnings("WeakerAccess")
@Slf4j
@Component
public class RuleSetExecutor {

    /**
     * Default constructor; initializes the object.
     */
    RuleSetExecutor() {
    }

    @Autowired
    RuleConditionMatcher ruleConditionMatcher;

    @Autowired
    RuleActionHandler ruleActionHandler;

    /**
     * Execute the given rule set against the given staging post.
     *
     * @param ruleSet The rule set to execute.
     * @param stagingPost The staging post upon which to evaluate the given rules.
     */
    public final void execute(RuleSet ruleSet, StagingPost stagingPost) {
        //
        // execute each rule in the rule set against the staging post
        //
        Set<Rule> rules = ruleSet.getRules();
        if (isEmpty(rules)) {
            log.warn("Skipping empty rule set: ruleSetId={}", ruleSet.getId());
        } else {
            log.debug("Executing ruleSetId={}, ruleSetName={}, stagingPostHash={}, username={}",
                    ruleSet.getId(), ruleSet.getName(), stagingPost.getPostHash(), stagingPost.getUsername());
            ruleSet.getRules()
                    .forEach(rule -> execute(rule, stagingPost));
        }
    }

    private void execute(Rule rule, StagingPost stagingPost) {
        long ruleId = rule.getId();
        log.debug("Executing ruleId={}, ruleName={}, postHash={}, username={}, queueId={}, subscriptionid={}",
                ruleId, rule.getName(), stagingPost.getPostHash(), stagingPost.getUsername(), stagingPost.getQueueId(),
                stagingPost.getSubscriptionId()
        );
        //
        // evaluate the match
        //
        boolean isMatch = ruleConditionMatcher.evaluateStagingPost(rule, stagingPost);
        //
        // if the rule is a match, perform the actions
        //
        if (isMatch) {
            log.debug("Rule match: ruleId={}, ruleName={}, postHash={}, username={}, queueId={}, subscriptionId={}",
                    ruleId, rule.getName(), stagingPost.getPostHash(), stagingPost.getUsername(), stagingPost.getQueueId(),
                    stagingPost.getSubscriptionId());
            ruleActionHandler.invokeActions(rule, stagingPost);
        }
    }

    @Override
    public final String toString() {
        return "RuleSetExecutor{" +
                "ruleConditionMatcher=" + ruleConditionMatcher +
                ", ruleActionHandler=" + ruleActionHandler +
                '}';
    }
}
