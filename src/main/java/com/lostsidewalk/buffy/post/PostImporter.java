package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.Importer;
import com.lostsidewalk.buffy.query.QueryDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.lostsidewalk.buffy.post.PostImporter.PostResolution.ADD;
import static com.lostsidewalk.buffy.post.PostImporter.PostResolution.SKIP;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;
import static org.apache.commons.lang3.StringUtils.EMPTY;


@Slf4j
@Component
public class PostImporter {

    @Autowired
    StagingPostDao stagingPostDao;

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
        int processorCt = Runtime.getRuntime().availableProcessors();
        log.info("Starting importer thread pool: processCount={}", processorCt);
        this.importerThreadPool = Executors.newFixedThreadPool(processorCt);
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
        // process errors
        //
        processErrors();
    }
    //
    // import success processing
    //
    private void startImportProcessor() {
        this.importProcessor = new Thread(() -> {
            log.info("Starting import processor at {}", Instant.now());
            int totalCt = 0, addCt = 0, skipCt = 0;
            while (true) {
                StagingPost sp = null;
                try {
                    sp = articleQueue.take();
                } catch (InterruptedException ignored) {
                    // ignored
                }
                if (sp != null) {
                    cleanStagingPost(sp);
                    try {
                        PostResolution resolution = processStagingPost(sp);

                        if (resolution == SKIP) {
                            skipCt++;
                        } else if (resolution == ADD) {
                            addCt++;
                        }
                        totalCt++;
                    } catch (Exception e) {
                        log.error("Something horrible happened while processing an import: {}", e.getMessage());
                    }
                }
                if (totalCt % 100 == 0) {
                    log.info("Import processor metrics: total={}, add={}, skip={}", totalCt, addCt, skipCt);
                }
            }
        });
        this.importProcessor.start();
    }

    private void cleanStagingPost(StagingPost stagingPost) {
        if (stagingPost.getPostDesc() == null) {
            stagingPost.setPostDesc(EMPTY);
        }
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
    //
    // import error processing
    //
    private void processErrors() {
        log.info("Processing {} import errors", size(errorQueue));
        errorQueue.forEach(this::logFailure);
    }

    private void logFailure(Throwable t) {
        log.error("Import error={}", t.getMessage());
    }
}
