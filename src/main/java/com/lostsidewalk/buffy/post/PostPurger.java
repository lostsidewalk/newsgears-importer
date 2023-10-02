package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataConflictException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.queue.QueueDefinitionDao;
import com.lostsidewalk.buffy.subscription.SubscriptionMetricsDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

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
        super();
    }

    /**
     * Initializes the PostPurger after construction.
     * Logs an informational message to indicate the purger has been constructed.
     */
    @PostConstruct
    protected void postConstruct() {
        log.info("Purger constructed");
    }

    /**
     * Purges archived staging posts based on the maximum post age configuration.
     *
     * @return The number of purged archived posts.
     * @throws DataAccessException If there is an issue accessing the data.
     * @throws DataUpdateException If there is an issue updating the data.
     */
    @SuppressWarnings("unused")
    public int purgeArchivedPosts() throws DataAccessException, DataUpdateException {
        log.debug("Purging ARCHIVED staging posts, params={}", this.configProps);
        return stagingPostDao.purgeArchivedPosts(this.configProps.getMaxPostAge());
    }

    /**
     * Marks idle posts for archival based on configured maximum unread and read ages.
     *
     * @return The number of marked idle posts for archival.
     * @throws DataAccessException If there is an issue accessing the data.
     * @throws DataUpdateException If there is an issue updating the data.
     */
    @SuppressWarnings("unused")
    public long markIdlePostsForArchive() throws DataAccessException, DataUpdateException {
        log.debug("Marking idle posts for archival, params={}", this.configProps);
        Map<String, List<Long>> idleStagingPosts = stagingPostDao.findAllIdle(configProps.getMaxUnreadAge(), configProps.getMaxReadAge());
        int ct = 0;
        for (Map.Entry<String, List<Long>> e : idleStagingPosts.entrySet()) {
            ct += stagingPostDao.archiveByIds(e.getKey(), e.getValue());
        }
        return ct;
    }

    /**
     * Purges deleted queues.
     *
     * @return The number of purged deleted queues.
     * @throws DataAccessException If there is an issue accessing the data.
     * @throws DataUpdateException If there is an issue updating the data.
     */
    @SuppressWarnings("unused")
    public long purgeDeletedQueues() throws DataAccessException, DataUpdateException {
        log.debug("Purging DELETED queues, params={}", this.configProps);
        return queueDefinitionDao.purgeDeleted();
    }

    /**
     * Purges orphaned query metrics.
     *
     * @return The number of purged orphaned query metrics.
     * @throws DataAccessException If there is an issue accessing the data.
     * @throws DataUpdateException If there is an issue updating the data.
     */
    @SuppressWarnings("unused")
    public long purgeOrphanedQueryMetrics() throws DataAccessException, DataUpdateException {
        log.debug("Purging ORPHANED query metrics, params={}", this.configProps);
        return subscriptionMetricsDao.purgeOrphaned();
    }
}
