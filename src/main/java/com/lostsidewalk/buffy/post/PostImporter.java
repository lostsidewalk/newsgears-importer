package com.lostsidewalk.buffy.post;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.Importer;
import com.lostsidewalk.buffy.Importer.ImportResult;
import com.lostsidewalk.buffy.query.QueryDefinition;
import com.lostsidewalk.buffy.query.QueryDefinitionDao;
import com.lostsidewalk.buffy.query.QueryMetrics;
import com.lostsidewalk.buffy.query.QueryMetricsDao;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;


@Slf4j
@Component
public class PostImporter {

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    QueryDefinitionDao queryDefinitionDao;

    @Autowired
    QueryMetricsDao queryMetricsDao;

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
            doImport(queryDefinitionDao.findAllActive());
        } catch (Exception e) {
            log.error("Something horrible happened while during the scheduled import: {}", e.getMessage(), e);
        }
    }

    @SuppressWarnings("unused")
    public void doImport(List<QueryDefinition> queryDefinitions) throws DataAccessException, DataUpdateException {
        if (isEmpty(queryDefinitions)) {
            log.info("No queries defined, terminating the import process early.");
            return;
        }

        if (isEmpty(importers)) {
            log.info("No importers defined, terminating the import process early.");
            return;
        }
        //
        // run the importers in a FJP to populate the article queue
        //
        List<ImportResult> allImportResults = synchronizedList(new ArrayList<>(size(importers)));
        CountDownLatch latch = new CountDownLatch(size(importers));
        importers.forEach(importer -> importerThreadPool.submit(() -> {
            log.info("Starting importerId={} with {} active query definitions", importer.getImporterId(), size(queryDefinitions));
            try {
                ImportResult importResult = importer.doImport(queryDefinitions);
                allImportResults.add(importResult);
            } catch (Exception e) {
                log.error("Something horrible happened on importerId={} due to: {}", importer.getImporterId(), e.getMessage(), e);
            }
            log.info("Completed importerId={}", importer.getImporterId());
            latch.countDown();
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Import process interrupted due to: {}", e.getMessage());
        }
        //
        // process errors
        //
        processErrors();
        //
        // process import results
        //
        processImportResults(ImmutableList.copyOf(allImportResults));
    }

    private void processImportResults(List<ImportResult> importResults) throws DataAccessException, DataUpdateException {
        Map<Long, Set<StagingPost>> importSetByQueryId = new HashMap<>();
        for (ImportResult importResult : importResults) {
            ImmutableSet<StagingPost> importSet = ImmutableSet.copyOf(importResult.getImportSet());
            for (StagingPost s : importSet) {
                importSetByQueryId.computeIfAbsent(s.getQueryId(), v -> new HashSet<>()).add(s);
            }
        }
        for (ImportResult importResult : importResults) {
            List<QueryMetrics> queryMetrics = ImmutableList.copyOf(importResult.getQueryMetrics());
            for (QueryMetrics q : queryMetrics) {
                Set<StagingPost> queryImportSet = importSetByQueryId.get(q.getQueryId());
                procesQueryImportSet(q, queryImportSet);
            }
        }
    }

    enum StagingPostResolution {
        PERSISTED,
        SKIP_ALREADY_EXISTS,
    }

    private void procesQueryImportSet(QueryMetrics queryMetrics, Set<StagingPost> importSet) throws DataAccessException, DataUpdateException {
        int persistCt = 0;
        for (StagingPost sp : importSet) {
            StagingPostResolution resolution = processStagingPost(sp);
            if (resolution == StagingPostResolution.PERSISTED) {
                persistCt++;
            }
        }
        processQueryMetrics(queryMetrics, persistCt);
    }

    private StagingPostResolution processStagingPost(StagingPost stagingPost) throws DataAccessException, DataUpdateException {
        // compute a hash of the post, attempt to find it in the data source;
        if (find(stagingPost)) {
            // log if present,
            logAlreadyExists(stagingPost);
            logSkipImport(stagingPost);
            return StagingPostResolution.SKIP_ALREADY_EXISTS;
        } else {
            persistStagingPost(stagingPost);
            return StagingPostResolution.PERSISTED;
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

    private void persistStagingPost(StagingPost stagingPost) throws DataAccessException, DataUpdateException {
        log.debug("Persisting staging post from importerDesc={}, hash={}", stagingPost.getImporterDesc(), stagingPost.getPostHash());
        stagingPostDao.add(stagingPost);
    }

    private void processQueryMetrics(QueryMetrics queryMetrics, int persistCt) throws DataAccessException, DataUpdateException {
        log.debug("Persisting query metrics: queryId={}, importCt={}, importTimestamp={}", queryMetrics.getQueryId(), queryMetrics.getImportCt(), queryMetrics.getImportTimestamp());
        queryMetrics.setPersistCt(persistCt);
        queryMetricsDao.add(queryMetrics);
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
