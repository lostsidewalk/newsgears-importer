package com.lostsidewalk.buffy.rule;

import com.lostsidewalk.buffy.post.StagingPost;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
        log.info("Executing ruleSetId={}, ruleSetName={}, stagingPostId={}, username={}",
                ruleSet.getId(), ruleSet.getName(), stagingPost.getId(), stagingPost.getUsername());
        //
        // execute each rule in the rule set against the staging post
        //
        ruleSet.getRules()
                .forEach(rule -> execute(rule, stagingPost));
    }

    private void execute(Rule rule, StagingPost stagingPost) {
        long ruleId = rule.getId();
        log.info("Executing ruleId={}, ruleName={}, stagingPostId={}, username={}, queueId={}, subscriptionid={}",
                ruleId, rule.getName(), stagingPost.getId(), stagingPost.getUsername(), stagingPost.getQueueId(),
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
            log.info("Rule match: ruleId={}, ruleName={}, stagingPostId={}, username={}, queueId={}, subscriptionId={}",
                    ruleId, rule.getName(), stagingPost.getId(), stagingPost.getUsername(), stagingPost.getQueueId(),
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
