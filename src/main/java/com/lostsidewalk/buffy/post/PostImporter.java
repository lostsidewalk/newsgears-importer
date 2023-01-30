package com.lostsidewalk.buffy.post;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.Importer;
import com.lostsidewalk.buffy.query.QueryDefinition;
import com.lostsidewalk.buffy.query.QueryDefinitionDao;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static com.lostsidewalk.buffy.post.PostImporter.PostResolution.ADD;
import static com.lostsidewalk.buffy.post.PostImporter.PostResolution.SKIP;
import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
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
    private BlockingQueue<StagingPost> articleQueue;

    @Autowired
    private BlockingQueue<Throwable> errorQueue;

    @Autowired
    List<Importer> importers;

    private Thread importProcessor;

    private ExecutorService importerThreadPool;

    @PostConstruct
    public void postConstruct() {
        log.info("Importers constructed, importerCt={}", size(importers));
        //
        // start thread process successful imports
        //
        startImportProcessor();
        int processorCt = min(size(importers), getRuntime().availableProcessors() - 1);
        log.info("Starting importer thread pool: processCount={}", processorCt);
        this.importerThreadPool = newFixedThreadPool(processorCt, new ThreadFactoryBuilder().setNameFormat("post-importer-%d").build());
    }

    @SuppressWarnings("unused")
    public Health health() {
        boolean processorIsRunning = this.importProcessor.isAlive();
        boolean importerPoolIsShutdown = this.importerThreadPool.isShutdown();

        if (processorIsRunning && !importerPoolIsShutdown) {
            return Health.up().build();
        } else {
            return Health.down()
                    .withDetail("importProcessorIsRunning", processorIsRunning)
                    .withDetail("importerPoolIsShutdown", importerPoolIsShutdown)
                    .build();
        }
    }

    @SuppressWarnings("unused")
    public void doImport() {
        try {
            doImport(queryDefinitionDao.findAllActive());
        } catch (Exception e) {
            log.error("Something horrible happened while fetching queries to import: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    public void doImport(List<QueryDefinition> queryDefinitions) {
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
        CountDownLatch latch = new CountDownLatch(size(importers));
        importers.forEach(importer -> importerThreadPool.submit(() -> {
            log.info("Starting importerId={} with {} active query definitions", importer.getImporterId(), size(queryDefinitions));
            try {
                importer.doImport(queryDefinitions);
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
        // update query definitions
        //
        updateQueryDefinitions(queryDefinitions);
        //
        // process errors
        //
        processErrors();
    }
    //
    // import success processing
    //
    private static final Logger importProcessorLog = LoggerFactory.getLogger("importProcessor");
    private void startImportProcessor() {
        log.info("Starting import processor at {}", Instant.now());
        this.importProcessor = new Thread(() -> {
            int totalCt = 0, addCt = 0, skipCt = 0;
            while (true) {
                StagingPost sp = null;
                try {
                    sp = articleQueue.take();
                } catch (InterruptedException ignored) {
                    // ignored
                }
                if (sp != null) {
                    try {
                        PostResolution resolution = processStagingPost(sp);

                        if (resolution == SKIP) {
                            skipCt++;
                        } else if (resolution == ADD) {
                            addCt++;
                        }
                        totalCt++;
                    } catch (Exception e) {
                        importProcessorLog.error("Something horrible happened while processing an import: {}", e.getMessage());
                    }
                }
                importProcessorLog.debug("Import processor metrics: total={}, add={}, skip={}", totalCt, addCt, skipCt);
            }
        });
        this.importProcessor.start();
    }

    enum PostResolution {
        SKIP, ADD
    }

    private PostResolution processStagingPost(StagingPost stagingPost) throws DataAccessException {
        // compute a hash of the post, attempt to find it in the data source;
        if (find(stagingPost)) {
            // log if present,
            logAlreadyExists(stagingPost);
            logSkipImport(stagingPost);
            return SKIP;
        } else {
            persistStagingPost(stagingPost);
            return ADD;
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

    private void updateQueryDefinitions(List<QueryDefinition> queryDefinitions) {
        log.info("Updating query definitions"); // TODO: implement this method
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
