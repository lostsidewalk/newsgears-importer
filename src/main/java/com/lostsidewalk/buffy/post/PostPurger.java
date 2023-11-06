package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.queue.QueueDefinitionDao;
import com.lostsidewalk.buffy.subscription.SubscriptionMetricsDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Component responsible for purging archived posts, marking idle posts for archival,
 * and purging deleted queues and orphaned query metrics based on configured properties.
 */
@Slf4j
@Component
public class PostPurger {

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    SubscriptionMetricsDao subscriptionMetricsDao;

    @Autowired
    QueueDefinitionDao queueDefinitionDao;

    @Autowired
    PostPurgerConfigProps configProps;

    /**
     * Default constructor; initializes the object.
     */
    PostPurger() {
    }

    /**
     * Initializes the PostPurger after construction.
     * Logs an informational message to indicate the purger has been constructed.
     */
    @PostConstruct
    protected static void postConstruct() {
        log.info("Purger constructed");
    }

    /**
     * Purges archived staging posts based on the maximum post age configuration.
     *
     * @return The number of purged archived posts.
     * @throws DataAccessException If there is an issue accessing the data.
     */
    @SuppressWarnings("unused")
    public final int purgeArchivedPosts() throws DataAccessException {
        log.debug("Purging ARCHIVED staging posts");
        return stagingPostDao.purgeArchivedPosts();
    }

    /**
     * Marks idle posts for archival based on configured maximum unread and read ages.
     *
     * @return The number of marked idle posts for archival.
     * @throws DataUpdateException If there is an issue updating the data.
     */
    @SuppressWarnings("unused")
    public final long markIdlePostsForArchive() throws DataUpdateException {
        log.debug("Marking idle posts for archival, params={}", configProps);
        return stagingPostDao.markIdlePostsForArchive(configProps.getMaxUnreadAge(), configProps.getMaxReadAge());
    }

    /**
     * Purges deleted queues.
     *
     * @return The number of purged deleted queues.
     * @throws DataAccessException If there is an issue accessing the data.
     */
    @SuppressWarnings("unused")
    public final long purgeDeletedQueues() throws DataAccessException {
        log.debug("Purging DELETED queues, params={}", configProps);
        return queueDefinitionDao.purgeDeleted();
    }

    /**
     * Purges orphaned query metrics.
     *
     * @return The number of purged orphaned query metrics.
     * @throws DataAccessException If there is an issue accessing the data.
     */
    @SuppressWarnings("unused")
    public final long purgeOrphanedQueryMetrics() throws DataAccessException {
        log.debug("Purging ORPHANED query metrics, params={}", configProps);
        return subscriptionMetricsDao.purgeOrphaned();
    }

    @Override
    public final String toString() {
        return "PostPurger{" +
                "stagingPostDao=" + stagingPostDao +
                ", subscriptionMetricsDao=" + subscriptionMetricsDao +
                ", queueDefinitionDao=" + queueDefinitionDao +
                ", configProps=" + configProps +
                '}';
    }
}
