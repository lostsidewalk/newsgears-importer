package com.lostsidewalk.buffy.post;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataConflictException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.discovery.FeedDiscoveryInfo;
import com.lostsidewalk.buffy.importer.Importer;
import com.lostsidewalk.buffy.importer.Importer.ImportResult;
import com.lostsidewalk.buffy.rule.RuleSet;
import com.lostsidewalk.buffy.rule.RuleSetDao;
import com.lostsidewalk.buffy.rule.RuleSetExecutor;
import com.lostsidewalk.buffy.subscription.SubscriptionDefinition;
import com.lostsidewalk.buffy.subscription.SubscriptionDefinitionDao;
import com.lostsidewalk.buffy.subscription.SubscriptionMetrics;
import com.lostsidewalk.buffy.subscription.SubscriptionMetricsDao;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Lists.partition;
import static com.lostsidewalk.buffy.post.ImportScheduler.ImportSchedule.importScheduleNamed;
import static com.lostsidewalk.buffy.post.PostArchiver.archive;
import static com.lostsidewalk.buffy.post.PostImporter.StagingPostResolution.*;
import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.commons.collections4.CollectionUtils.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Component responsible for importing posts based on scheduled subscriptions.
 * This class coordinates the import process for various subscriptions and handles error processing.
 */
@SuppressWarnings("WeakerAccess")
@Slf4j
@Component
public class PostImporter {

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    SubscriptionDefinitionDao subscriptionDefinitionDao;

    @Autowired
    SubscriptionMetricsDao subscriptionMetricsDao;

    @Autowired
    RuleSetDao ruleSetDao;

    @Autowired
    List<Importer> importers;

    @Autowired
    RuleSetExecutor ruleSetExecutor;

    @Autowired
    PostArchiver postArchiver;

    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    private ExecutorService importerThreadPool;

    /**
     * Default constructor; initializes the object.
     */
    PostImporter() {
    }

    /**
     * Initializes the PostImporter after construction.
     * It sets up the importer thread pool based on available processors.
     */
    @PostConstruct
    protected final void postConstruct() {
        log.info("Importers constructed, importerCt={}", size(importers));
        //
        // setup the importer thread pool
        //
        int availableProcessors = getRuntime().availableProcessors();
        int processorCt = availableProcessors > 1 ? min(size(importers), availableProcessors - 1) : availableProcessors;
        processorCt = processorCt >= 2 ? processorCt - 1 : processorCt; // account for the import processor thread
        log.info("Starting importer thread pool: processCount={}", processorCt);
        importerThreadPool = newFixedThreadPool(processorCt, new ThreadFactoryBuilder().setNameFormat("post-importer-%d").build());
    }

    /**
     * Checks the health of the importer.
     *
     * @return Health status of the importer.
     */
    @SuppressWarnings("unused")
    public final Health health() {
        boolean importerPoolIsShutdown = importerThreadPool.isShutdown();

        if (importerPoolIsShutdown) {
            return Health.down()
                    .withDetail("importerPoolIsShutdown", false)
                    .build();
        } else {
            return Health.up().build();
        }
    }

    /**
     * Initiates the post import process.
     */
    @SuppressWarnings("unused")
    public final void doImport() {
        try {
            List<SubscriptionDefinition> scheduledSubscriptions = getScheduledSubscriptions();
            doImport(scheduledSubscriptions);
        } catch (DataAccessException | DataUpdateException | DataConflictException | RuntimeException e) {
            log.error("Something horrible happened while during the scheduled import: {}", e.getMessage(), e);
        }
    }

    private List<SubscriptionDefinition> getScheduledSubscriptions() throws DataAccessException {
        LocalDateTime now = LocalDateTime.now();
        return subscriptionDefinitionDao.findAllActive().stream().filter(qd -> scheduleMatches(qd.getImportSchedule(), now)).toList();
    }

    private static boolean scheduleMatches(String schedule, LocalDateTime localDateTime) {
        if (isBlank(schedule)) {
            return false;
        }
        return importScheduleNamed(schedule)
                .map(importSchedule -> importSchedule.matches(localDateTime))
                .orElse(false);
    }

    /**
     * Initiates the post import process for a given list of subscription definitions.
     *
     * @param subscriptionDefinitions List of subscription definitions to import.
     * @throws DataAccessException  If there is an issue accessing the data.
     * @throws DataUpdateException  If there is an issue updating the data.
     * @throws DataConflictException If there is a duplicate key.
     */
    public final void doImport(List<SubscriptionDefinition> subscriptionDefinitions) throws DataAccessException, DataUpdateException, DataConflictException {
        doImport(subscriptionDefinitions, Map.of());
    }

    /**
     * Initiates the post import process for a given list of subscription definitions and a cache of feed discovery information.
     *
     * @param allSubscriptionDefinitions List of all subscription definitions to import.
     * @param discoveryCache             Cache of feed discovery information.
     * @throws DataAccessException  If there is an issue accessing the data.
     * @throws DataUpdateException  If there is an issue updating the data.
     * @throws DataConflictException If there is a duplicate key.
     */
    @SuppressWarnings("unused")
    public final void doImport(List<SubscriptionDefinition> allSubscriptionDefinitions, Map<String, FeedDiscoveryInfo> discoveryCache) throws DataAccessException, DataUpdateException, DataConflictException {
        if (isEmpty(allSubscriptionDefinitions)) {
            log.info("No subscriptions defined, terminating the import process early.");
            return;
        }

        if (isEmpty(importers)) {
            log.info("No importers defined, terminating the import process early.");
            return;
        }
        //
        // partition queries into chunks
        //
        List<List<SubscriptionDefinition>> subscriptionBundles = partition(allSubscriptionDefinitions, 100);
        int bundleIdx = 1;
        for (List<SubscriptionDefinition> subscriptionBundle : subscriptionBundles) {
            //
            // run the importers to populate the article queue
            //
            List<ImportResult> allImportResults = synchronizedList(new ArrayList<>(size(importers)));
            CountDownLatch latch = new CountDownLatch(size(importers));
            log.info("Starting import of bundle index {}", bundleIdx);
            importers.forEach(importer -> importerThreadPool.submit(() -> {
                log.info("Starting importerId={} with {} bundled subscriptions", importer.getImporterId(), size(subscriptionBundle));
                try {
                    ImportResult importResult = importer.doImport(subscriptionBundle, discoveryCache);
                    allImportResults.add(importResult);
                } catch (RuntimeException e) {
                    log.error("Something horrible happened on importerId={} due to: {}", importer.getImporterId(), e.getMessage(), e);
                }
                log.info("Completed importerId={} for all bundled subscriptions", importer.getImporterId());
                latch.countDown();
            }));
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("Import process interrupted due to: {}", e.getMessage());
                break;
            }
            //
            // process errors
            //
            processErrors();
            //
            // process import results (persist staging posts and query metrics)
            //
            processImportResults(copyOf(allImportResults));
            //
            // increment bundle index (for logging)
            //
            bundleIdx++;
        }
    }

    /**
     * Processes the import results, including persisting staging posts and query metrics.
     *
     * @param importResults List of import results.
     * @throws DataAccessException  If there is an issue accessing the data.
     * @throws DataUpdateException  If there is an issue updating the data.
     * @throws DataConflictException If there is a duplicate key.
     */
    @SuppressWarnings({"unused", "MethodWithMultipleLoops"})
    public final void processImportResults(Iterable<? extends ImportResult> importResults) throws DataAccessException, DataUpdateException, DataConflictException {
        // subscription Id -> collection of newly imported staging posts
        Map<Long, Set<StagingPost>> importSetBySubscriptionId = new HashMap<>(16);
        // subscription Id -> rule set to execute on all newly imported staging posts in this subscription
        Map<Long, List<RuleSet>> ruleSetsBySubscriptionId = new HashMap<>(16);
        // (first pass, map up import sets and rule sets)
        for (ImportResult importResult : importResults) {
            ImmutableSet<StagingPost> importSet = ImmutableSet.copyOf(importResult.getImportSet());
            for (StagingPost stagingPost : importSet) {
                String username = stagingPost.getUsername();
                long subscriptionId = stagingPost.getSubscriptionId();
                long queueId = stagingPost.getQueueId();
                // add to importSetBySubscriptionId
                importSetBySubscriptionId.computeIfAbsent(
                        subscriptionId,
                        v -> new HashSet<>(size(importSet))
                ).add(stagingPost);
                // add to ruleSetsBySubscriptionId
                if (!ruleSetsBySubscriptionId.containsKey(subscriptionId)) {
                    List<RuleSet> ruleSets = ruleSetDao.findBySubscriptionId(username, subscriptionId);
                    ruleSetsBySubscriptionId.put(subscriptionId, isNotEmpty(ruleSets) ? ruleSets : emptyList());
                }
            }
        }
        // (second pass, perform processing)
        for (ImportResult importResult : importResults) {
            List<SubscriptionMetrics> subscriptionMetrics = copyOf(importResult.getSubscriptionMetrics());
            for (SubscriptionMetrics subscriptionMetric : subscriptionMetrics) {
                Set<StagingPost> subscriptionImportSet = importSetBySubscriptionId.get(subscriptionMetric.getSubscriptionId());
                if (isNotEmpty(subscriptionImportSet)) {
                    processSubscriptionImportSet(
                            // metric
                            subscriptionMetric,
                            // import set
                            subscriptionImportSet,
                            // subscription rule sets
                            ruleSetsBySubscriptionId.get(subscriptionMetric.getSubscriptionId())
                    );
                }
            }
        }
    }

    /**
     * Enumeration representing possible resolutions for staging posts.
     */
    enum StagingPostResolution {
        PERSISTED,
        ARCHIVED,
        SKIP_ALREADY_EXISTS,
    }
    //
    // import set processing
    //
    private void processSubscriptionImportSet(SubscriptionMetrics queryMetrics, Iterable<? extends StagingPost> importSet, List<? extends RuleSet> subscriptionRuleSets)
            throws DataAccessException, DataUpdateException, DataConflictException
    {
        int persistCt = 0;
        int skipCt = 0;
        int archiveCt = 0;
        for (StagingPost sp : importSet) {
            StagingPostResolution resolution = processStagingPost(sp, subscriptionRuleSets);
            if (resolution == PERSISTED) {
                persistCt++;
            } else if (resolution == SKIP_ALREADY_EXISTS) {
                skipCt++;
            } else if (resolution == ARCHIVED) {
                archiveCt++;
            }
        }
        log.debug("Persisting query metrics: subscriptionId={}, importCt={}, importTimestamp={}, persistCt={}, skipCt={}, archiveCt={}",
                queryMetrics.getSubscriptionId(), queryMetrics.getImportCt(), queryMetrics.getImportTimestamp(), persistCt, skipCt, archiveCt);
        queryMetrics.setPersistCt(persistCt);
        queryMetrics.setSkipCt(skipCt);
        queryMetrics.setArchiveCt(archiveCt);
        subscriptionMetricsDao.add(queryMetrics);
    }
    //
    // staging post processing
    //
    private StagingPostResolution processStagingPost(
            StagingPost stagingPost,
            @SuppressWarnings("TypeMayBeWeakened") List<? extends RuleSet> subscriptionRuleSets
    ) throws DataAccessException, DataUpdateException, DataConflictException {
        // compute a hash of the post, attempt to find it in the data source;
        if (stagingPostDao.checkExists(stagingPost.getPostHash())) {
            // log if present,
            log.debug("Staging post already exists, hash={}", stagingPost.getPostHash());
            log.debug("Skipping staging post from importerDesc={}, hash={}", stagingPost.getImporterDesc(), stagingPost.getPostHash());
            return SKIP_ALREADY_EXISTS;
        } else {
            //
            // rules engine
            //
            if (isNotEmpty(subscriptionRuleSets)) {
                subscriptionRuleSets
                        .forEach(r -> ruleSetExecutor.execute(r, stagingPost));
            }
            //
            // archiver
            //
            boolean isArchived = archive(stagingPost);
            //
            // persister
            //
            stagingPostDao.add(stagingPost);

            return isArchived ? ARCHIVED : PERSISTED;
        }
    }
    //
    // import error processing
    //
    private void processErrors() {
        log.info("Processing {} import errors", size(errorQueue));
        Collection<Throwable> errorList = new ArrayList<>(errorQueue.size());
        errorQueue.drainTo(errorList);
        errorList.forEach(t -> log.error("Import error={}", t.getMessage()));
    }

    @Override
    public final String toString() {
        return "PostImporter{" +
                "stagingPostDao=" + stagingPostDao +
                ", subscriptionDefinitionDao=" + subscriptionDefinitionDao +
                ", subscriptionMetricsDao=" + subscriptionMetricsDao +
                ", ruleSetDao=" + ruleSetDao +
                ", importers=" + importers +
                ", ruleSetExecutor=" + ruleSetExecutor +
                ", postArchiver=" + postArchiver +
                ", errorQueue=" + errorQueue +
                ", importerThreadPool=" + importerThreadPool +
                '}';
    }
}
