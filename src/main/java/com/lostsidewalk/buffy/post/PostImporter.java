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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
        for (ImportResult importResult : importResults) {
            processStagingPosts(ImmutableSet.copyOf((importResult.getImportSet())));
            persistQueryMetrics(ImmutableList.copyOf(importResult.getQueryMetrics()));
        }
    }

    private void processStagingPosts(Set<StagingPost> importSet) throws DataAccessException {
        for (StagingPost sp : importSet) {
            processStagingPost(sp);
        }
    }

    private void processStagingPost(StagingPost stagingPost) throws DataAccessException {
        // compute a hash of the post, attempt to find it in the data source;
        if (find(stagingPost)) {
            // log if present,
            logAlreadyExists(stagingPost);
            logSkipImport(stagingPost);
        } else {
            persistStagingPost(stagingPost);
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

    private void persistStagingPost(StagingPost stagingPost) throws DataAccessException {
        log.debug("Persisting staging post from importerDesc={}, hash={}", stagingPost.getImporterDesc(), stagingPost.getPostHash());
        stagingPostDao.add(stagingPost);
    }

    private void persistQueryMetrics(List<QueryMetrics> queryMetrics) throws DataAccessException, DataUpdateException {
        for (QueryMetrics qm : queryMetrics) {
            log.debug("Persisting query metrics: queryId={}, importCt={}, importTimestamp={}", qm.getQueryId(), qm.getImportCt(), qm.getImportTimestamp());
            queryMetricsDao.add(qm); // TODO: make this a batch operation
        }
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
