package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Queue;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;


@Slf4j
@Component
@Configuration
public class PostImporter {

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    private Queue<StagingPost> articleQueue;

    @Autowired
    private Queue<Throwable> errorQueue;

    @Autowired
    List<Importer> importers;

    @PostConstruct
    public void postConstruct() {
        log.info("Post importer constructed, importersCt={}", CollectionUtils.size(importers));
    }
    //
    // perform the import process once per hour
    //
    @Scheduled(fixedDelayString = "${post.importer.fixed-delay}", timeUnit = HOURS)
    public void doImport() {
        log.info("Post import process starting at {}", FastDateFormat.getDateTimeInstance(FastDateFormat.MEDIUM, FastDateFormat.MEDIUM).format(new Date()));
        //
        // populate the queue
        //
        if (isNotEmpty(importers)) {
            importers.forEach(this::doImport);
        }
        //
        // process successful imports
        //
        processImports();
        //
        // process errors
        //
        processErrors();
        //
        //
        //
        this.articleQueue.clear();
    }

    private void doImport(Importer importer) {
        log.info("Post importer started for {}", importer.getImporterId());
        importer.doImport();
    }
    //
    // import success processing
    //
    private int totalCt = 0, addCt = 0, skipCt = 0;

    private void processImports() {
        log.info("Post importer processing {} articles at {}", this.articleQueue.size(), Instant.now());
        StagingPost sp;
        while ((sp = this.articleQueue.peek()) != null) {
            this.clean(sp);
            this.success(sp);
            this.articleQueue.remove();
        }
        log.info("Post importer processed {} articles ({} added, {} skipped) at {}", this.totalCt, this.addCt, this.skipCt, Instant.now());
    }

    private void clean(StagingPost stagingPost) {
        if (stagingPost.getPostDesc() == null) {
            stagingPost.setPostDesc(EMPTY);
        }
    }

    private void success(StagingPost stagingPost) {
        // compute a hash of the post, attempt to find it in the data source;
        if (find(stagingPost)) {
            // log if present,
            this.logAlreadyExists(stagingPost);
            this.logSkipImport(stagingPost);
            this.skipCt++;
        } else {
            this.persist(stagingPost);
            this.addCt++;
        }
        this.totalCt++;
    }

    private boolean find(StagingPost stagingPost) {
        log.debug("Attempting to locate staging post, hash={}", stagingPost.getPostHash());
        return stagingPostDao.checkExists(stagingPost.getPostHash());
    }

    private void logAlreadyExists(StagingPost stagingPost) {
        log.debug("Staging post already exists, hash={}", stagingPost.getPostHash());
    }

    private void logSkipImport(StagingPost stagingPost) {
        log.debug("Skipping staging post from importerDesc={}, hash={}", stagingPost.getImporterDesc(), stagingPost.getPostHash());
    }

    private void persist(StagingPost stagingPost) {
        log.debug("Persisting staging post from importerDesc={}, hash={}", stagingPost.getImporterDesc(), stagingPost.getPostHash());
        this.stagingPostDao.add(stagingPost);
    }
    //
    // import error processing
    //
    private void processErrors() {
        this.errorQueue.forEach(this::logFailure);
    }

    private void logFailure(Throwable t) {
        log.error("Import error={}", t.getMessage());
    }
}
