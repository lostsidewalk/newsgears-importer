package com.lostsidewalk.buffy.rule;

import com.google.gson.Gson;
import com.lostsidewalk.buffy.post.StagingPost;
import com.lostsidewalk.buffy.rule.RuleAction.ActionType;
import com.lostsidewalk.buffy.rule.WebHookRequest.WebHookRequestException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.lostsidewalk.buffy.post.StagingPost.PostReadStatus.READ;
import static com.lostsidewalk.buffy.post.StagingPost.PostReadStatus.READ_LATER;
import static com.lostsidewalk.buffy.rule.WebHookUtils.postWebHook;

/**
 * This class is responsible for handling rule actions such as webhooks.
 */
@SuppressWarnings({"WeakerAccess", "ClassWithMultipleLoggers"})
@Slf4j
@Component
public class RuleActionHandler {

    /**
     * Default constructor; initialized the object.
     */
    RuleActionHandler() {
    }

    private final BlockingQueue<WebHookRequest> webHookRequestQueue = new LinkedBlockingQueue<>();

    @Value("${newsgears.userAgent}")
    String newsgearsUserAgent;

    private Thread webHookProcessor;

    @PostConstruct
    final void postConstruct() {
        log.info("Rule action handler constructed");
        //
        // start a thread to process web hook requests
        //
        startWebHookProcessor();
    }
    //
    // web hook request processing
    //
    // TODO: this needs to be multi-threaded
    //
    private static final Logger webHookProcessorLog = LoggerFactory.getLogger("webHookProcessor");
    private void startWebHookProcessor() {
        log.info("Starting web hook request processor at {}", Instant.now());
        webHookProcessor = new Thread(() -> {
            int totalCt = 0;
            while (true) {
                WebHookRequest wh = null;
                try {
                    wh = webHookRequestQueue.take();
                    postWebHook(wh, newsgearsUserAgent);
                    totalCt++;
                } catch (InterruptedException e) {
                    // ignored
                } catch (WebHookRequestException e) {
                    webHookProcessorLog.error("Something horrible happened while posting a web hook to URL={}: {}", (wh == null ? null : wh.url), e.getMessage());
                }
                webHookProcessorLog.debug("Web hook processor metrics: total={}", totalCt);
            }
        });
    }

    /**
     * Check the health of the rule action handler.
     *
     * @return A Health object indicating the health status of the web hook processor.
     */
    @SuppressWarnings("unused")
    public final Health health() {
        boolean processorIsRunning = webHookProcessor.isAlive();

        if (processorIsRunning) {
            return Health.up().build();
        } else {
            return Health.down()
                    .withDetail("webHookProcessorIsRunning", processorIsRunning)
                    .build();
        }
    }

    //
    // this is the main point of entry from the rules engine; this method sequences the actions and invokes them all
    // in the correct order.
    //

    final void invokeActions(Rule rule, StagingPost stagingPost) {
        long ruleId = rule.getId();
        //
        // sort the rule actions by sequence and invoke each on the staging post
        //
        rule.getActions().stream()
                .sorted(Comparator.comparing(RuleAction::getSequence))
                .forEach(a -> invokeAction(ruleId, stagingPost, a.getActionType(), a.getParameters()));
    }

    //
    // this method determines the correct action to take based on the action type;
    // in the case of a web hook, a new web hook request is submitted to the queue;
    // in the case of post status updates, those updates are made directly.
    //

    private void invokeAction(long ruleId, StagingPost stagingPost, ActionType actionType, Object ... parameters) {
        log.info("Invoking action, ruleId={}, stagingPostId={}, actionType={}, parameters={}",
                ruleId, stagingPost.getId(), actionType, parameters);
        switch (actionType) {
            case WEBHOOK -> webHookRequestQueue.add(new WebHookRequest(
                    // url
                    parameters[0].toString(),
                    // payload
                    serialize(stagingPost),
                    // username,
                    parameters.length > 1 ? parameters[1].toString() : null,
                    // password
                    parameters.length > 2 ? parameters[2].toString() : null
            ));
            case MARK_AS_READ -> stagingPost.setPostReadStatus(READ);
            case MARK_AS_READ_LATER -> stagingPost.setPostReadStatus(READ_LATER);
        }
    }

    private static final Gson GSON = new Gson();

    private static String serialize(StagingPost stagingPost) {
        return GSON.toJson(stagingPost);
    }

    //
    //
    //

    @Override
    public final String toString() {
        return "RuleActionHandler{" +
                "requestQueue=" + webHookRequestQueue +
                ", newsgearsUserAgent='" + newsgearsUserAgent + '\'' +
                ", webHookProcessor=" + webHookProcessor +
                '}';
    }
}
