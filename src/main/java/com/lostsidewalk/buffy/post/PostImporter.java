package com.lostsidewalk.buffy.post;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.discovery.FeedDiscoveryInfo;
import com.lostsidewalk.buffy.importer.Importer;
import com.lostsidewalk.buffy.importer.Importer.ImportResult;
import com.lostsidewalk.buffy.post.StagingPost.PostPubStatus;
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

import static com.google.common.collect.ImmutableList.copyOf;
import static com.lostsidewalk.buffy.post.ImportScheduler.ImportSchedule.scheduleNamed;
import static com.lostsidewalk.buffy.post.PostImporter.StagingPostResolution.*;
import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
import static java.util.Calendar.DATE;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.commons.collections4.CollectionUtils.*;
import static org.apache.commons.lang3.StringUtils.isBlank;


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
    private BlockingQueue<Throwable> errorQueue;

    @Autowired
    List<Importer> importers;

    private ExecutorService importerThreadPool;

    @PostConstruct
    public void postConstruct() {
        log.info("Importers constructed, importerCt={}", size(importers));
        //
        // setup the importer thread pool
        //
        int availableProcessors = getRuntime().availableProcessors();
        int processorCt = availableProcessors > 1 ? min(size(importers), availableProcessors - 1) : availableProcessors;
        processorCt = processorCt >= 2 ? processorCt - 1 : processorCt; // account for the import processor thread
        log.info("Starting importer thread pool: processCount={}", processorCt);
        this.importerThreadPool = newFixedThreadPool(processorCt, new ThreadFactoryBuilder().setNameFormat("post-importer-%d").build());
    }

    @SuppressWarnings("unused")
    public Health health() {
        boolean importerPoolIsShutdown = this.importerThreadPool.isShutdown();

        if (!importerPoolIsShutdown) {
            return Health.up().build();
        } else {
            return Health.down()
                    .withDetail("importerPoolIsShutdown", false)
                    .build();
        }
    }

    @SuppressWarnings("unused")
    public void doImport() {
        try {
            List<SubscriptionDefinition> scheduledSubscriptions = getScheduledSubscriptions();
            doImport(scheduledSubscriptions);
        } catch (Exception e) {
            log.error("Something horrible happened while during the scheduled import: {}", e.getMessage(), e);
        }
    }

    private List<SubscriptionDefinition> getScheduledSubscriptions() throws DataAccessException {
        LocalDateTime now = LocalDateTime.now();
        return subscriptionDefinitionDao.findAllActive().stream().filter(qd -> scheduleMatches(qd.getImportSchedule(), now)).toList();
    }

    private boolean scheduleMatches(String schedule, LocalDateTime localDateTime) {
        if (isBlank(schedule)) {
            return false;
        }
        return scheduleNamed(schedule)
                .map(s -> s.matches(localDateTime))
                .orElse(false);
    }

    public void doImport(List<SubscriptionDefinition> subscriptionDefinitions) throws DataAccessException, DataUpdateException {
        doImport(subscriptionDefinitions, Map.of());
    }

    @SuppressWarnings("unused")
    public void doImport(List<SubscriptionDefinition> allSubscriptionDefinitions, Map<String, FeedDiscoveryInfo> discoveryCache) throws DataAccessException, DataUpdateException {
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
        List<List<SubscriptionDefinition>> queryBundles = Lists.partition(allSubscriptionDefinitions, 100);
        int bundleIdx = 1;
        for (List<SubscriptionDefinition> queryBundle : queryBundles) {
            //
            // run the importers to populate the article queue
            //
            List<ImportResult> allImportResults = synchronizedList(new ArrayList<>(size(importers)));
            CountDownLatch latch = new CountDownLatch(size(importers));
            log.info("Starting import of bundle index {}", bundleIdx);
            importers.forEach(importer -> importerThreadPool.submit(() -> {
                log.info("Starting importerId={} with {} bundled subscriptions", importer.getImporterId(), size(queryBundle));
                try {
                    ImportResult importResult = importer.doImport(queryBundle, discoveryCache);
                    allImportResults.add(importResult);
                } catch (Exception e) {
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

    @SuppressWarnings("unused")
    public void processImportResults(List<ImportResult> importResults) throws DataAccessException, DataUpdateException {
        Map<Long, Set<StagingPost>> importSetByQueryId = new HashMap<>();
        for (ImportResult importResult : importResults) {
            ImmutableSet<StagingPost> importSet = ImmutableSet.copyOf(importResult.getImportSet());
            for (StagingPost s : importSet) {
                importSetByQueryId.computeIfAbsent(s.getSubscriptionId(), v -> new HashSet<>()).add(s);
            }
        }
        for (ImportResult importResult : importResults) {
            List<SubscriptionMetrics> queryMetrics = copyOf(importResult.getSubscriptionMetrics());
            for (SubscriptionMetrics q : queryMetrics) {
                Set<StagingPost> queryImportSet = importSetByQueryId.get(q.getSubscriptionId());
                if (isNotEmpty(queryImportSet)) {
                    processQueryImportSet(q, queryImportSet);
                }
            }
        }
    }

    enum StagingPostResolution {
        PERSISTED,
        ARCHIVED,
        SKIP_ALREADY_EXISTS,
    }

    private void processQueryImportSet(SubscriptionMetrics queryMetrics, Set<StagingPost> importSet) throws DataAccessException, DataUpdateException {
        int persistCt = 0;
        int skipCt = 0;
        int archiveCt = 0;
        for (StagingPost sp : importSet) {
            StagingPostResolution resolution = processStagingPost(sp);
            if (resolution == PERSISTED) {
                persistCt++;
            } else if (resolution == SKIP_ALREADY_EXISTS) {
                skipCt++;
            } else if (resolution == ARCHIVED) {
                archiveCt++;
            }
        }
        processQueryMetrics(queryMetrics, persistCt, skipCt, archiveCt);
    }

    private StagingPostResolution processStagingPost(StagingPost stagingPost) throws DataAccessException, DataUpdateException {
        // compute a hash of the post, attempt to find it in the data source;
        if (find(stagingPost)) {
            // log if present,
            logAlreadyExists(stagingPost);
            logSkipImport(stagingPost);
            return SKIP_ALREADY_EXISTS;
        } else {
            Calendar cal = Calendar.getInstance();
            cal.add(DATE, -90);
            Date archiveDate = cal.getTime();
            // archive criteria:
            //
            // (1) missing both publish timestamp and last updated timestamp (spam injected into the feed)
            boolean hasTimestamp = (stagingPost.getPublishTimestamp() != null || stagingPost.getLastUpdatedTimestamp() != null);
            // (2) published before the archive date without any updates
            boolean publishedTooLongAgoNoUpdates = stagingPost.getLastUpdatedTimestamp() == null && stagingPost.getPublishTimestamp() != null && stagingPost.getPublishTimestamp().before(archiveDate);
            // (3) most recent update is before the archive date
            boolean mostRecentUpdateIsTooOld = stagingPost.getLastUpdatedTimestamp() != null && stagingPost.getLastUpdatedTimestamp().before(archiveDate);
            boolean doArchive = (!hasTimestamp || publishedTooLongAgoNoUpdates || mostRecentUpdateIsTooOld);
            persistStagingPost(stagingPost, doArchive);

            return doArchive ? ARCHIVED : PERSISTED;
        }
    }

    private boolean find(StagingPost stagingPost) throws DataAccessException {
        log.debug("Attempting to locate staging post, hash={}", stagingPost.getPostHash());
        return stagingPostDao.checkExists(stagingPost.getPostHash());
    }

    private void logAlreadyExists(StagingPost stagingPost) {
        log.debug("Staging post already exists, hash={}", stagingPost.getPostHash());
    }

    private void logSkipImport(StagingPost stagingPost) {
        log.debug("Skipping staging post from importerDesc={}, hash={}", stagingPost.getImporterDesc(), stagingPost.getPostHash());
    }

    private void persistStagingPost(StagingPost stagingPost, boolean doArchive) throws DataAccessException, DataUpdateException {
        log.debug("Persisting staging post from importerDesc={}, hash={}, doArchive={}", stagingPost.getImporterDesc(), stagingPost.getPostHash(), doArchive);
        if (doArchive) {
            stagingPost.setPostPubStatus(PostPubStatus.ARCHIVED);
        }
        stagingPostDao.add(stagingPost);
    }

    private void processQueryMetrics(SubscriptionMetrics queryMetrics, int persistCt, int skipCt, int archiveCt) throws DataAccessException, DataUpdateException {
        log.debug("Persisting query metrics: subscriptionId={}, importCt={}, importTimestamp={}, persistCt={}, skipCt={}, archiveCt={}",
                queryMetrics.getSubscriptionId(), queryMetrics.getImportCt(), queryMetrics.getImportTimestamp(), persistCt, skipCt, archiveCt);
        queryMetrics.setPersistCt(persistCt);
        queryMetrics.setSkipCt(skipCt);
        queryMetrics.setArchiveCt(archiveCt);
        subscriptionMetricsDao.add(queryMetrics);
    }
    //
    // import error processing
    //
    private void processErrors() {
        log.info("Processing {} import errors", size(errorQueue));
        List<Throwable> errorList = new ArrayList<>(errorQueue.size());
        errorQueue.drainTo(errorList);
        errorList.forEach(this::logFailure);
    }

    private void logFailure(Throwable t) {
        log.error("Import error={}", t.getMessage());
    }
}
