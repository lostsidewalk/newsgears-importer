package com.lostsidewalk.buffy.post;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Calendar;
import java.util.Date;

import static java.util.Calendar.DATE;

@Slf4j
@Component
class PostArchiver {

    static boolean archive(StagingPost stagingPost) {
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
        log.debug("Persisting staging post from importerDesc={}, hash={}, doArchive={}", stagingPost.getImporterDesc(), stagingPost.getPostHash(), doArchive);
        if (doArchive) {
            stagingPost.setArchived(true);
        }

        return doArchive;
    }
}
